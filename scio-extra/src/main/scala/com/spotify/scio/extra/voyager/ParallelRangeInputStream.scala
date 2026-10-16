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

import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing
import org.slf4j.LoggerFactory

import java.io.{EOFException, IOException, InputStream, InterruptedIOException}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, SeekableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

/** A remote file that can be read starting at any offset. */
private[voyager] trait RangeSource {

  /** Stable identity of this version of the file, used to key the spill cache. */
  def id: String
  def size: Long

  /** Opens a channel positioned at `offset`. */
  def openAt(offset: Long): ReadableByteChannel

  /** Fails if the file changed since this source was created. */
  def verifyUnchanged(): Unit
}

private[voyager] object RangeSource {

  /** A [[RangeSource]] backed by Beam [[FileSystems]], e.g. GCS. */
  def apply(uri: URI): RangeSource = {
    val meta = FileSystems.matchSingleFileSpec(uri.toString)
    new RangeSource {
      override val id: String = s"$uri|${meta.sizeBytes()}|${meta.lastModifiedMillis()}"
      override val size: Long = meta.sizeBytes()

      override def openAt(offset: Long): ReadableByteChannel = {
        val channel = FileSystems.open(meta.resourceId())
        if (offset > 0) {
          channel match {
            case c: SeekableByteChannel => c.position(offset)
            case _                      =>
              channel.close()
              throw new IOException(s"$uri does not support reading from an offset")
          }
        }
        channel
      }

      override def verifyUnchanged(): Unit = {
        val now = FileSystems.matchSingleFileSpec(uri.toString)
        if (
          now.sizeBytes() != meta.sizeBytes() || now.lastModifiedMillis() != meta
            .lastModifiedMillis()
        ) {
          throw new IOException(s"$uri was modified while it was being read")
        }
      }
    }
  }
}

final private[voyager] case class ParallelDownloadSettings(
  parallelism: Int = 16,
  chunkSize: Int = 64 << 20,
  // chunks fetched or buffered ahead of the reader; bounds heap use to roughly window * chunkSize
  window: Int = 16,
  // a chunk fails only after this many consecutive attempts that made no progress
  maxAttemptsWithoutProgress: Int = 30,
  initialBackoffMillis: Long = 1000L,
  maxBackoffMillis: Long = 60000L,
  // spill completed chunks to local disk so a failed load can resume without re-fetching them.
  // Off by default: fetching waits on disk writes, capping throughput at disk speed, and a
  // restarted worker may not see the spilled files.
  spill: Boolean = false,
  // chunks waiting to be spilled; fetching blocks when full, so spilling is never skipped
  maxPendingSpills: Int = 8,
  spillRoot: Path = Paths.get(System.getProperty("java.io.tmpdir"))
)

/**
 * An [[InputStream]] over a [[RangeSource]] that fetches fixed-size chunks in parallel and serves
 * them in order. Each chunk retries independently with backoff, resuming from the last byte
 * received, so transient failures such as rate limiting never surface to the reader. Completed
 * chunks are optionally spilled to local disk, and chunks already spilled by an earlier attempt are
 * read from disk instead of being fetched again.
 */
private[voyager] class ParallelRangeInputStream(
  source: RangeSource,
  settings: ParallelDownloadSettings = ParallelDownloadSettings()
) extends InputStream {
  import ParallelRangeInputStream._

  private val chunkSize = settings.chunkSize
  private val numChunks = ((source.size + chunkSize - 1) / chunkSize).toInt

  private val spillDir: Option[Path] =
    if (settings.spill && numChunks > 0) {
      val key = Hashing
        .sha256()
        .hashString(s"${source.id}|$chunkSize", StandardCharsets.UTF_8)
        .toString
        .take(16)
      Some(Files.createDirectories(settings.spillRoot.resolve(s"voyager-chunks-$key")))
    } else {
      None
    }

  private val fetchPool = Executors.newFixedThreadPool(settings.parallelism, daemonThreads("fetch"))
  private val spillPool = Executors.newSingleThreadExecutor(daemonThreads("spill"))
  private val spillPermits = new Semaphore(settings.maxPendingSpills)

  private val futures = new Array[CompletableFuture[Array[Byte]]](numChunks)
  private var nextToSubmit = 0
  private var currentIdx = -1
  private var current: Array[Byte] = Array.emptyByteArray
  private var pos = 0
  private var eof = false
  @volatile private var closed = false

  /** Number of chunks fetched from the source rather than from the spill directory. */
  private[voyager] val fetchedChunks = new AtomicInteger()

  private def chunkLength(i: Int): Int =
    math.min(chunkSize.toLong, source.size - i.toLong * chunkSize).toInt

  private def spillFile(i: Int): Option[Path] = spillDir.map(_.resolve(f"chunk-$i%06d"))

  private def submitAhead(): Unit =
    while (nextToSubmit < numChunks && nextToSubmit <= currentIdx + settings.window) {
      val i = nextToSubmit
      futures(i) = CompletableFuture.supplyAsync(() => loadChunk(i), fetchPool)
      nextToSubmit += 1
    }

  private def loadChunk(i: Int): Array[Byte] = {
    val len = chunkLength(i)
    val spilled =
      spillFile(i).filter(f => Files.isRegularFile(f) && Files.size(f) == len).flatMap { f =>
        try Some(Files.readAllBytes(f))
        catch {
          case NonFatal(e) =>
            Log.warn(s"Failed to read spilled chunk $f, fetching it again", e)
            None
        }
      }
    spilled.getOrElse {
      val bytes = fetch(i.toLong * chunkSize, len)
      fetchedChunks.incrementAndGet()
      spill(i, bytes)
      bytes
    }
  }

  private def fetch(start: Long, len: Int): Array[Byte] = {
    val buf = new Array[Byte](len)
    var filled = 0
    var failures = 0
    var backoff = settings.initialBackoffMillis
    while (filled < len) {
      if (closed) throw new IOException("Stream closed")
      val before = filled
      try {
        val channel = source.openAt(start + filled)
        try {
          while (filled < len) {
            val n = channel.read(ByteBuffer.wrap(buf, filled, len - filled))
            if (n < 0) {
              throw new EOFException(s"Unexpected end of ${source.id} at ${start + filled}")
            }
            filled += n
          }
        } finally {
          try channel.close()
          catch { case NonFatal(_) => }
        }
      } catch {
        case NonFatal(e) =>
          if (filled > before) {
            failures = 0
            backoff = settings.initialBackoffMillis
          }
          failures += 1
          if (failures >= settings.maxAttemptsWithoutProgress) {
            throw new IOException(
              s"Failed to read ${source.id} at ${start + filled} after $failures attempts",
              e
            )
          }
          val sleep = backoff / 2 + ThreadLocalRandom.current().nextLong(backoff / 2 + 1)
          Log.warn(
            s"Failed to read ${source.id} at ${start + filled} (attempt $failures), " +
              s"retrying in ${sleep}ms",
            e
          )
          Thread.sleep(sleep)
          backoff = math.min(backoff * 2, settings.maxBackoffMillis)
      }
    }
    buf
  }

  private def spill(i: Int, bytes: Array[Byte]): Unit = spillFile(i).foreach { target =>
    spillPermits.acquire()
    try {
      spillPool.execute { () =>
        var tmp: Path = null
        try {
          tmp = Files.createTempFile(target.getParent, target.getFileName.toString, ".tmp")
          Files.write(tmp, bytes)
          Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE)
        } catch {
          case NonFatal(e) =>
            Log.warn(s"Failed to spill chunk to $target", e)
            if (tmp != null) {
              try Files.deleteIfExists(tmp)
              catch { case NonFatal(_) => }
            }
        } finally {
          spillPermits.release()
        }
      }
    } catch {
      case e: RejectedExecutionException =>
        spillPermits.release()
        throw new IOException("Stream closed", e)
    }
  }

  private def advance(): Boolean = {
    if (currentIdx >= 0) futures(currentIdx) = null // let the chunk be collected
    currentIdx += 1
    if (currentIdx >= numChunks) {
      eof = true
      source.verifyUnchanged()
      false
    } else {
      submitAhead()
      current =
        try futures(currentIdx).get()
        catch {
          case e: ExecutionException =>
            throw new IOException(s"Failed to read chunk $currentIdx of ${source.id}", e.getCause)
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            throw new InterruptedIOException()
        }
      pos = 0
      true
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (closed) throw new IOException("Stream closed")
    if (len == 0) {
      0
    } else if (eof || (pos == current.length && !advance())) {
      -1
    } else {
      val n = math.min(len, current.length - pos)
      System.arraycopy(current, pos, b, off, n)
      pos += n
      n
    }
  }

  override def read(): Int = {
    val one = new Array[Byte](1)
    if (read(one, 0, 1) == -1) -1 else one(0) & 0xff
  }

  override def close(): Unit = if (!closed) {
    closed = true
    futures.foreach(f => if (f != null) f.cancel(true))
    fetchPool.shutdownNow()
    spillPool.shutdown() // let pending spills finish
  }
}

private[voyager] object ParallelRangeInputStream {
  private val Log = LoggerFactory.getLogger(classOf[ParallelRangeInputStream])

  private val threadIds = new AtomicInteger()

  private def daemonThreads(name: String): ThreadFactory = { (r: Runnable) =>
    val t = new Thread(r, s"voyager-$name-${threadIds.incrementAndGet()}")
    t.setDaemon(true)
    t
  }
}
