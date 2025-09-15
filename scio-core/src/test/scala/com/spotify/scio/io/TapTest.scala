/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.io

import com.spotify.scio._
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.coders.ByteArrayCoder
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.util.{CoderUtils, SerializableUtils}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.{FileUtils, IOUtils}

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

trait TapSpec extends PipelineSpec {
  def verifyTap[T: Coder](tap: Tap[T], expected: Set[T]): Unit = {
    SerializableUtils.ensureSerializable(tap)
    tap.value.toSet shouldBe expected
    val sc = ScioContext()
    tap.open(sc) should containInAnyOrder(expected)
    sc.run().waitUntilFinish() // block non-test runner
    ()
  }

  def runWithInMemoryFuture[T](fn: ScioContext => ClosedTap[T]): Tap[T] =
    runWithFuture(ScioContext.forTest())(fn)

  def runWithFileFuture[T](fn: ScioContext => ClosedTap[T]): Tap[T] =
    runWithFuture(ScioContext())(fn)

  def runWithFuture[T](sc: ScioContext)(fn: ScioContext => ClosedTap[T]): Tap[T] = {
    val f = fn(sc)
    val scioResult = sc.run().waitUntilFinish() // block non-test runner
    scioResult.tap(f)
  }

  def withTempDir(test: File => Any): Unit = {
    val dir = Files.createTempDirectory("scio-test-").toFile
    try {
      test(dir)
    } finally {
      FileUtils.deleteDirectory(dir)
    }
  }
}

final case class TapRecord(i: Int, s: String)

class TapTest extends TapSpec {
  val records: Set[TapRecord] = Set(1, 2, 3).map(i => TapRecord(i, i.toString))

  "Future" should "support saveAsInMemoryTap" in {
    val t = runWithInMemoryFuture(_.parallelize(records).saveAsInMemoryTap)
    verifyTap(t, records)
  }

  it should "support materialize" in {
    val t = runWithFileFuture(_.parallelize(records).materialize)
    verifyTap(t, records)
  }

  it should "support saveAsTextFile" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq("a", "b", "c"))
        .saveAsTextFile(dir.getAbsolutePath)
    }
    verifyTap(t, Set("a", "b", "c"))
  }

  it should "support reading compressed text files" in withTempDir { dir =>
    val nFiles = 10
    val nLines = 100
    val data = Array.fill(nFiles)(Array.fill(nLines)(UUID.randomUUID().toString))

    Seq(
      CompressorStreamFactory.GZIP -> ".gz",
      CompressorStreamFactory.BZIP2 -> ".bz2"
    ).map { case (cType, ext) =>
      val compressDir = new File(dir, cType)
      compressDir.mkdir()
      val suffix = ".txt" + ext
      (0 until nFiles).foreach { f =>
        val file = new File(compressDir, f"part-$f%05d-of-$nFiles%05d$suffix%s")
        val os = new CompressorStreamFactory()
          .createCompressorOutputStream(cType, new FileOutputStream(file))
        IOUtils.write(data(f).mkString("", "\n", "\n"), os, StandardCharsets.UTF_8)
        os.close()
      }

      val params = TextIO.ReadParam(compression = Compression.detect(suffix), suffix = suffix)
      verifyTap(TextTap(compressDir.getAbsolutePath, params), data.flatten.toSet)
    }
  }

  it should "keep parent after Tap.map" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq(1, 2, 3))
        .saveAsTextFile(dir.getAbsolutePath)
    }.map(_.toInt)
    verifyTap(t, Set(1, 2, 3))
    t.isInstanceOf[Tap[Int]] shouldBe true
    t.parent.get.isInstanceOf[Tap[_]] shouldBe true
  }

  it should "keep parent after Tap.flatMap" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq("12", "34", "56"))
        .saveAsTextFile(dir.getAbsolutePath)
    }.flatMap(_.split("").map(_.toInt))
    verifyTap(t, Set(1, 2, 3, 4, 5, 6))
    t.isInstanceOf[Tap[Int]] shouldBe true
    t.parent.get.isInstanceOf[TextTap] shouldBe true
  }

  it should "support waitForResult" in {
    val sc = ScioContext()
    val f = sc.parallelize(1 to 10).materialize
    val scioResult = sc.run().waitUntilDone()
    scioResult.tap(f).value.toSet shouldBe (1 to 10).toSet
  }

  it should "support nested waitForResult" in {
    val sc = ScioContext()
    val f = sc.parallelize(1 to 10).materialize
    val scioResult = sc.run().waitUntilDone()
    scioResult.tap(f).value.toSet shouldBe (1 to 10).toSet
  }

  it should "materialize elements whose encoding matches a compression algorithm's" in {
    val element = (1, 0, s"/var/folders/g_/${"a" * 100}", false)
    val coder = CoderMaterializer.beamWithDefault(Coder[(Int, Int, String, Boolean)])
    val encoded = CoderUtils.encodeToByteArray(
      ByteArrayCoder.of(),
      CoderUtils.encodeToByteArray(coder, element),
      org.apache.beam.sdk.coders.Coder.Context.NESTED
    )

    // Assert that the encoding coincidentally matches the Deflate compression signature
    CompressorStreamFactory.detect(
      new BufferedInputStream(new ByteArrayInputStream(encoded))
    ) shouldBe CompressorStreamFactory.DEFLATE

    // Assert that we can still materialize it in a tap
    val (_, tapped) = runWithLocalOutput(_.parallelize(Seq(element)))
    tapped should contain only element
  }
}
