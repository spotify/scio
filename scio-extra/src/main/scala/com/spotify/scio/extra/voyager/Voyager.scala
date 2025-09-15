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
import com.spotify.voyager.jni.{Index, StringIndex}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ConcurrentHashMap

/**
 * Represents the base URI for a voyager index, either on a local or a remote file system. For
 * remote file systems, the `path` should be in the form 'scheme://<bucket>/<path>/'. For local
 * files, it should be in the form '/<path>/'. The `uri` specified represents the directory where
 * the `index.hnsw` and `names.json` are.
 */
final case class VoyagerUri(value: URI) extends AnyVal {

  import VoyagerUri._

  def exists(implicit remoteFileUtil: RemoteFileUtil): Boolean = {
    if (ScioUtil.isLocalUri(value)) {
      VoyagerFiles.exists(f => Paths.get(value.resolve(f)).toFile.exists())
    } else {
      VoyagerFiles.exists(f => remoteFileUtil.remoteExists(value.resolve(f)))
    }
  }
}

object VoyagerUri {
  def apply(value: String): VoyagerUri =
    new VoyagerUri(URI.create(value.stripSuffix("/") + "/"))

  private[voyager] val IndexFile = "index.hnsw"
  private[voyager] val NamesFile = "names.json"

  private[voyager] val VoyagerFiles: Seq[String] = Seq(IndexFile, NamesFile)

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
  @transient private lazy val index: StringIndex = indexSettings match {
    case VoyagerReader.ProvidedSettings(space, numDimensions, storageDataType) =>
      StringIndex.load(
        indexFile.toString,
        namesFile.toString,
        space,
        numDimensions,
        storageDataType
      )
    case VoyagerReader.MetadataSettings =>
      StringIndex.load(
        indexFile.toString,
        namesFile.toString
      )
  }

  /**
   * Gets maxNumResults nearest neighbors for vector v using ef (where ef is the size of the dynamic
   * list for the nearest neighbors during search).
   */
  def getNearest(v: Array[Float], maxNumResults: Int, ef: Int): Array[VoyagerResult] = {
    val queryResults = index.query(v, maxNumResults, ef)
    queryResults.getNames
      .zip(queryResults.getDistances)
      .map { case (name, distance) => VoyagerResult(name, distance) }
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
    val indexUri = uri.value.resolve(VoyagerUri.IndexFile)
    val namesUri = uri.value.resolve(VoyagerUri.NamesFile)

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
