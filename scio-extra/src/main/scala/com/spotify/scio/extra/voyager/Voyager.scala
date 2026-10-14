/*
 * Copyright 2023 Spotify AB.
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

import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import com.spotify.scio.values.SideInput
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.Index
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
 * Represents the base URI for a voyager index, either on a local or a remote file system. For
 * remote file systems, the `path` should be in the form 'scheme://<bucket>/<path>/'. For local
 * files, it should be in the form '/<path>/'. The `uri` specified represents the directory where
 * the index and names files are.
 *
 * @param value
 *   The base URI of the directory containing the index files.
 * @param indexFile
 *   The filename of the HNSW index file. Defaults to `index.hnsw`.
 * @param namesFile
 *   The filename of the names JSON file. Defaults to `names.json`.
 */
final case class VoyagerUri(
  value: URI,
  indexFile: String = VoyagerUri.IndexFile,
  namesFile: String = VoyagerUri.NamesFile
) {

  private[voyager] def voyagerFiles: Seq[String] = Seq(indexFile, namesFile)

  def exists(implicit remoteFileUtil: RemoteFileUtil): Boolean = {
    if (ScioUtil.isLocalUri(value)) {
      voyagerFiles.exists(f => Paths.get(value.resolve(f)).toFile.exists())
    } else {
      voyagerFiles.exists(f => remoteFileUtil.remoteExists(value.resolve(f)))
    }
  }
}

object VoyagerUri {
  def apply(
    value: String,
    indexFile: String,
    namesFile: String
  ): VoyagerUri =
    new VoyagerUri(URI.create(value.stripSuffix("/") + "/"), indexFile, namesFile)

  def apply(value: String): VoyagerUri =
    new VoyagerUri(URI.create(value.stripSuffix("/") + "/"))

  private[voyager] val IndexFile = "index.hnsw"
  private[voyager] val NamesFile = "names.json"

}

/** Result of a voyager query */
final case class VoyagerResult(name: String, distance: Float)

class VoyagerWriter private[voyager] (
  indexFile: Path,
  namesFile: Path,
  indexSettings: VoyagerWriter.IndexSettings
) {
  import VoyagerWriter._

  def write(vectors: Iterable[(String, Array[Float])]): Unit = {
    val indexOutputStream = Files.newOutputStream(indexFile)
    val namesOutputStream = Files.newOutputStream(namesFile)

    val names = List.newBuilder[String]
    val index = new Index(
      indexSettings.space,
      indexSettings.numDimensions,
      indexSettings.indexM,
      indexSettings.efConstruction,
      indexSettings.randomSeed,
      indexSettings.maxElements,
      indexSettings.storageDataType
    )

    vectors.zipWithIndex
      .map { case ((name, vector), idx) => (name, vector, idx.toLong) }
      .grouped(ChunkSize)
      .map(_.unzip3)
      .foreach { case (ns, vs, is) =>
        names ++= ns
        index.addItems(vs.toArray, is.toArray, -1)
      }

    // save index
    index.saveIndex(indexOutputStream)
    index.close()
    // save names
    val json = names.result().mkString("[\"", "\",\"", "\"]")
    namesOutputStream.write(json.getBytes(StandardCharsets.UTF_8))
    // close
    indexOutputStream.close()
    namesOutputStream.close()
  }
}

private[voyager] object VoyagerWriter {

  private val ChunkSize: Int = 32786 // 2^15

  // same default as voyager Index constructor
  val DefaultIndexM: Long = 16L
  val DefaultEfConstruction: Long = 200L
  val DefaultRandomSeed: Long = 1L
  val DefaultMaxElements: Long = 1L
  val DefaultStorageDataType: StorageDataType = StorageDataType.Float32

  final case class IndexSettings(
    space: SpaceType,
    numDimensions: Int,
    indexM: Long = DefaultIndexM,
    efConstruction: Long = DefaultEfConstruction,
    randomSeed: Long = DefaultRandomSeed,
    maxElements: Long = DefaultMaxElements,
    storageDataType: StorageDataType = DefaultStorageDataType
  )

}

/**
 * Voyager reader class for nearest neighbor lookups. Supports looking up neighbors for a vector and
 * returning the string labels and distances associated.
 *
 * @param indexFile
 *   The `index.hnsw` file.
 * @param namesFile
 *   The `names.json` file.
 * @param indexSettings
 *   The voyager index settings, either provided or read from the index v2 metadata
 */
class VoyagerReader private[voyager] (
  indexFile: Path,
  namesFile: Path,
  indexSettings: VoyagerReader.IndexSettings
) {
  // load the native index and parse the names concurrently; neither depends on the other
  @transient private lazy val loaded: (Index, Array[String]) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val indexF = Future {
      indexSettings match {
        case VoyagerReader.ProvidedSettings(space, numDimensions, storageDataType) =>
          Index.load(indexFile.toString, space, numDimensions, storageDataType)
        case VoyagerReader.MetadataSettings =>
          Index.load(indexFile.toString)
      }
    }
    val namesF = Future(VoyagerReader.readNames(namesFile))
    Await.result(indexF.zip(namesF), Duration.Inf)
  }
  private def index: Index = loaded._1
  private def names: Array[String] = loaded._2

  /**
   * Gets maxNumResults nearest neighbors for vector v using ef (where ef is the size of the dynamic
   * list for the nearest neighbors during search).
   */
  def getNearest(v: Array[Float], maxNumResults: Int, ef: Int): Array[VoyagerResult] = {
    val queryResults = index.query(v, maxNumResults, ef.toLong)
    val labels = queryResults.getLabels
    val distances = queryResults.getDistances
    Array.tabulate(labels.length)(i => VoyagerResult(names(labels(i).toInt), distances(i)))
  }
}

private[voyager] object VoyagerReader {
  sealed trait IndexSettings
  final case class ProvidedSettings(
    space: SpaceType,
    numDimensions: Int,
    storageDataType: StorageDataType
  ) extends IndexSettings
  case object MetadataSettings extends IndexSettings

  /**
   * Parses a voyager `names.json` file, a JSON list of strings. Voyager forbids backslashes and
   * double quotes in names, so no escape handling is needed. This is much faster than voyager's
   * Scanner-based `TinyJson.readStringList` for large name lists. The file is streamed in chunks,
   * so it may be larger than the 2GB JVM array limit.
   */
  private[voyager] def readNames(namesFile: Path): Array[String] = {
    val in = Files.newInputStream(namesFile)
    try parseNames(in)
    finally in.close()
  }

  private[voyager] def parseNames(bytes: Array[Byte]): Array[String] =
    parseNames(new ByteArrayInputStream(bytes))

  // parser states
  private val BeforeList = 0
  private val ListStart = 1 // after '[': expect a name or ']'
  private val InName = 2
  private val AfterName = 3 // expect ',' or ']'
  private val AfterComma = 4 // expect a name
  private val Done = 5

  private[voyager] def parseNames(
    in: InputStream,
    chunkSize: Int = 1 << 20
  ): Array[String] = {
    val Quote = '"'.toByte
    def isWs(b: Byte): Boolean = b == ' ' || b == '\n' || b == '\r' || b == '\t'

    val chunk = new Array[Byte](chunkSize)
    // holds the start of a name that spans chunk boundaries
    var carry = new Array[Byte](256)
    var carryLen = 0
    def appendCarry(from: Int, until: Int): Unit = {
      val len = until - from
      if (carryLen + len > carry.length) {
        carry = java.util.Arrays.copyOf(carry, math.max(carry.length * 2, carryLen + len))
      }
      System.arraycopy(chunk, from, carry, carryLen, len)
      carryLen += len
    }

    val names = Array.newBuilder[String]
    var state = BeforeList
    var pos = 0L // absolute offset, for error messages
    def fail(i: Int): Nothing =
      throw new IllegalArgumentException(s"Malformed voyager names list at byte ${pos + i}")

    var n = in.read(chunk)
    while (n != -1 && state != Done) {
      var i = 0
      while (i < n && state != Done) {
        if (state == InName) {
          val start = i
          while (i < n && chunk(i) != Quote) i += 1
          if (i < n) {
            names += (if (carryLen == 0) new String(chunk, start, i - start, StandardCharsets.UTF_8)
                      else {
                        appendCarry(start, i)
                        val s = new String(carry, 0, carryLen, StandardCharsets.UTF_8)
                        carryLen = 0
                        s
                      })
            state = AfterName
            i += 1
          } else {
            appendCarry(start, n)
          }
        } else {
          val b = chunk(i)
          if (!isWs(b)) {
            state = (state, b) match {
              case (BeforeList, '[')                     => ListStart
              case (ListStart | AfterComma, Quote)       => InName
              case (ListStart | AfterName, ']')          => Done
              case (AfterName, ',')                      => AfterComma
              case _                                     => fail(i)
            }
          }
          i += 1
        }
      }
      pos += n
      if (state != Done) n = in.read(chunk)
    }
    if (state != Done) fail(0)
    names.result()
  }
}

/**
 * Construction for a VoyagerSide input that leverages a synchronized map to ensure that the reader
 * is only loaded once per [[VoyagerUri]].
 */
private[voyager] class VoyagerSideInput(
  indexSettings: VoyagerReader.IndexSettings,
  remoteFileUtil: RemoteFileUtil,
  override val view: PCollectionView[VoyagerUri]
) extends SideInput[VoyagerReader] {

  import VoyagerSideInput._

  private def createReader(uri: VoyagerUri): VoyagerReader = {
    val indexUri = uri.value.resolve(uri.indexFile)
    val namesUri = uri.value.resolve(uri.namesFile)

    val (localIndex, localNames) = if (ScioUtil.isLocalUri(uri.value)) {
      (Paths.get(indexUri), Paths.get(namesUri))
    } else {
      val downloadedIndex = remoteFileUtil.download(indexUri)
      val downloadedNames = remoteFileUtil.download(namesUri)
      (downloadedIndex, downloadedNames)
    }
    new VoyagerReader(localIndex, localNames, indexSettings)
  }

  override def get[I, O](context: DoFn[I, O]#ProcessContext): VoyagerReader = {
    val uri = context.sideInput(view)
    VoyagerReaderSharedCache.computeIfAbsent(uri, createReader)
  }
}

private object VoyagerSideInput {
  // cache the VoyagerUri to VoyagerReader per JVM so workers with multiple
  // voyager side-input steps load the index only once
  @transient private lazy val VoyagerReaderSharedCache
    : ConcurrentHashMap[VoyagerUri, VoyagerReader] =
    new ConcurrentHashMap()
}
