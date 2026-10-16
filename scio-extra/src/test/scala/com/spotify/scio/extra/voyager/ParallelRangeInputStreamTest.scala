/*
 * Copyright 2026 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.extra.voyager

import com.spotify.voyager.jni.Index
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, IOException, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

class ParallelRangeInputStreamTest extends AnyFlatSpec with Matchers {

  /**
   * In-memory source. `failAt(offset, attempt)` is consulted on every read; returning true makes
   * that read throw, simulating rate limiting or a dropped connection.
   */
  class FakeSource(
    data: Array[Byte],
    failAt: (Long, Int) => Boolean = (_, _) => false,
    maxReadSize: Int = 1000,
    // drop every connection after this many successful reads
    dropAfterReads: Int = Int.MaxValue
  ) extends RangeSource {
    val opens = new AtomicInteger()
    val failures = new AtomicInteger()
    @volatile var changed = false
    override val id: String = s"fake-${data.length}-${System.identityHashCode(data)}"
    override def size: Long = data.length.toLong
    override def verifyUnchanged(): Unit = if (changed) throw new IOException("changed")
    override def openAt(offset: Long): ReadableByteChannel = {
      val attempt = opens.incrementAndGet()
      new ReadableByteChannel {
        private var pos = offset
        private var reads = 0
        override def read(dst: ByteBuffer): Int = {
          if (pos >= data.length) return -1
          if (failAt(pos, attempt) || reads >= dropAfterReads) {
            failures.incrementAndGet()
            throw new IOException(s"429 Too Many Requests at $pos")
          }
          val n = math.min(math.min(dst.remaining(), maxReadSize), data.length - pos.toInt)
          dst.put(data, pos.toInt, n)
          pos += n
          reads += 1
          n
        }
        override def isOpen: Boolean = true
        override def close(): Unit = ()
      }
    }
  }

  private def fast(s: ParallelDownloadSettings = ParallelDownloadSettings()) =
    s.copy(
      chunkSize = 4096,
      window = 4,
      parallelism = 4,
      initialBackoffMillis = 1,
      maxBackoffMillis = 4
    )

  private def readAll(in: InputStream, bufSize: Int = 777): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](bufSize)
    var n = in.read(buf)
    while (n != -1) { out.write(buf, 0, n); n = in.read(buf) }
    in.close()
    out.toByteArray
  }

  private val data = {
    val bytes = new Array[Byte](100 * 1000 + 17) // not a multiple of the chunk size
    new Random(1).nextBytes(bytes)
    bytes
  }

  "ParallelRangeInputStream" should "return the source bytes in order" in {
    for (bufSize <- Seq(1, 100, 4096, 10000)) {
      withClue(s"bufSize=$bufSize") {
        readAll(new ParallelRangeInputStream(new FakeSource(data), fast()), bufSize) shouldBe data
      }
    }
  }

  it should "handle empty and tiny sources" in {
    readAll(new ParallelRangeInputStream(new FakeSource(Array.empty), fast())) shouldBe empty
    readAll(new ParallelRangeInputStream(new FakeSource(Array[Byte](1, 2, 3)), fast())) shouldBe
      Array[Byte](1, 2, 3)
  }

  it should "retry failed reads and resume from the last byte received" in {
    val src = new FakeSource(data, failAt = (_, attempt) => attempt % 3 == 0)
    readAll(new ParallelRangeInputStream(src, fast())) shouldBe data
    src.failures.get() should be > 0
  }

  it should "not give up on a range that keeps making progress" in {
    // every connection drops after one small read, far more often than maxAttemptsWithoutProgress
    val src = new FakeSource(data, maxReadSize = 10, dropAfterReads = 1)
    readAll(
      new ParallelRangeInputStream(src, fast().copy(maxAttemptsWithoutProgress = 3))
    ) shouldBe data
    src.failures.get() should be > 3
  }

  it should "fail once a range stops making progress" in {
    val src = new FakeSource(data, failAt = (pos, _) => pos >= 50000)
    val in = new ParallelRangeInputStream(src, fast().copy(maxAttemptsWithoutProgress = 3))
    val e = the[IOException] thrownBy readAll(in)
    e.getMessage should include("Failed to read chunk")
  }

  it should "fail if the source changed while being read" in {
    val src = new FakeSource(data)
    val in = new ParallelRangeInputStream(src, fast())
    src.changed = true
    an[IOException] should be thrownBy readAll(in)
  }

  it should "reuse spilled chunks after a failed attempt" in {
    val root = Files.createTempDirectory("voyager-spill")
    val settings = fast().copy(spill = true, spillRoot = root, maxAttemptsWithoutProgress = 2)
    val numChunks = (data.length + 4095) / 4096

    // first attempt dies near the end
    val failing = new FakeSource(data, failAt = (pos, _) => pos >= data.length - 5000)
    an[IOException] should be thrownBy readAll(new ParallelRangeInputStream(failing, settings))

    // let pending spills land; then a healthy retry only fetches what is missing
    Thread.sleep(500)
    val healthy = new FakeSource(data) {
      override val id: String = failing.id
    }
    val in = new ParallelRangeInputStream(healthy, settings)
    readAll(in) shouldBe data
    in.fetchedChunks.get() should be < numChunks / 2
  }

  it should "feed Index.load in parallel ranges" in {
    val dir = Files.createTempDirectory("voyager-index")
    val file = dir.resolve("index.hnsw")
    val index = new Index(SpaceType.Cosine, 8, 16, 200, 1, 2000, StorageDataType.Float32)
    val rnd = new Random(2)
    val vectors = Array.fill(2000)(Array.fill(8)(rnd.nextFloat()))
    index.addItems(vectors, (0L until 2000L).toArray, -1)
    index.saveIndex(file.toString)
    index.close()

    val bytes = Files.readAllBytes(file)
    val flaky = new FakeSource(bytes, failAt = (_, attempt) => attempt % 4 == 0)
    val in = new ParallelRangeInputStream(flaky, fast())
    val loaded =
      try Index.load(in)
      finally in.close()
    loaded.query(vectors(42), 1).getLabels.head shouldBe 42L
    loaded.close()
  }

  "RangeSource" should "read Beam file system URIs from an offset" in {
    val file = Files.createTempFile("range-source", ".bin")
    Files.write(file, data)
    val in = new ParallelRangeInputStream(RangeSource(file.toUri), fast())
    readAll(in) shouldBe data
  }
}
