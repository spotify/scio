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
import com.spotify.voyager.jni.{Index, StringIndex}
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable

/**
 * Represents the base URI for a voyager index, either on a local or a remote file system. For
 * remote file systems, the `path` should be in the form 'scheme://<bucket>/<path>/'. For local
 * files, it should be in the form '/<path>/'. The `path` specified represents the directory where
 * the `index.hnsw` and `names.json` are.
 */
sealed trait VoyagerUri {
  def path: String
  private[voyager] def getReader(
    distanceMeasure: SpaceType,
    storageDataType: StorageDataType,
    dim: Int
  ): VoyagerReader
  private[voyager] def saveAndClose(voyagerWriter: VoyagerWriter): Unit
  private[voyager] def exists: Boolean
}

private[voyager] object VoyagerUri {

  def apply(path: String, opts: PipelineOptions): VoyagerUri = {
    if (ScioUtil.isLocalUri(new URI(path))) {
      LocalVoyagerUri(path)
    } else {
      val rfu: RemoteFileUtil = RemoteFileUtil.create(opts)
      RemoteVoyagerUri(path, rfu)
    }
  }

  def files: Seq[String] = Seq("index.hnsw", "names.json")
}

case class LocalVoyagerUri(path: String) extends VoyagerUri {
  override private[voyager] def getReader(
    distanceMeasure: SpaceType,
    storageType: StorageDataType,
    dim: Int
  ): VoyagerReader = {

    val indexFileName: String = path + "/index.hnsw"
    val namesFileName: String = path + "/names.json"
    new VoyagerReader(indexFileName, namesFileName, distanceMeasure, storageType, dim)
  }

  override private[voyager] def saveAndClose(w: VoyagerWriter): Unit = {
    w.save(path)
    w.close()
  }

  override private[voyager] def exists: Boolean =
    VoyagerUri.files.exists(f => new File(path + "/" + f).exists())
}

case class RemoteVoyagerUri(
  path: String,
  remoteFileUtil: RemoteFileUtil
) extends VoyagerUri {
  import RemoteVoyagerUri._

  override private[voyager] def getReader(
    distanceMeasure: SpaceType,
    storageType: StorageDataType,
    dim: Int
  ): VoyagerReader = {
    val indexFileName: String = remoteFileUtil.download(new URI(path + "/index.hnsw")).toString
    val namesFileName: String = remoteFileUtil.download(new URI(path + "/names.json")).toString
    new VoyagerReader(indexFileName, namesFileName, distanceMeasure, storageType, dim)
  }

  override private[voyager] def saveAndClose(w: VoyagerWriter): Unit = {
    val tempPath: Path = Files.createTempDirectory("voyager-")
    w.save(tempPath.toString)
    w.close()

    VoyagerUri.files.foreach { f =>
      val tf: Path = tempPath.resolve(f)
      remoteFileUtil.upload(Paths.get(tf.toString), new URI(path + "/" + f))
      logger.info(s"Uploaded Voyager $f file from $tempPath to $path/$f")
      Files.delete(tf)
    }
  }

  override private[voyager] def exists: Boolean =
    VoyagerUri.files.exists(f => remoteFileUtil.remoteExists(new URI(path + "/" + f)))
}

object RemoteVoyagerUri {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(classOf[RemoteVoyagerUri])

  def apply(path: String, options: PipelineOptions): RemoteVoyagerUri =
    RemoteVoyagerUri(path, RemoteFileUtil.create(options))
}

private[voyager] class VoyagerWriter(
  spaceType: SpaceType,
  storageDataType: StorageDataType,
  dim: Int,
  ef: Long = 200L,
  m: Long = 16L
) {
  import VoyagerWriter._

  private val namesOutput = mutable.ListBuffer.empty[String]

  private val index: Index =
    new Index(spaceType, dim, m, ef, RANDOM_SEED, CHUNK_SIZE, storageDataType)

  def write(vectors: Iterable[(String, Array[Float])]): Unit = {
    val nameVectorIndexIterator = vectors.iterator.zipWithIndex
      .map { case ((name, vector), idx) =>
        (name, vector, idx.longValue())
      }

    while (nameVectorIndexIterator.hasNext) {
      val (nameArray, vectorArray, indexArray) = nameVectorIndexIterator
        .take(CHUNK_SIZE)
        .toArray
        .unzip3

      index.addItems(vectorArray, indexArray, -1)
      namesOutput ++= nameArray
    }

    ()
  }

  def save(path: String): Unit = {
    val indexFileName: String = path + "/index.hnsw"
    val namesFileName: String = path + "/names.json"
    index.saveIndex(indexFileName)
    Files.write(
      Paths.get(namesFileName),
      namesOutput.mkString("[\"", "\",\"", "\"]").getBytes(StandardCharsets.UTF_8)
    )
    ()
  }

  def close(): Unit = {
    index.close()
    ()
  }

}

private object VoyagerWriter {
  val RANDOM_SEED: Long = 1L
  val CHUNK_SIZE: Int = 32786 // 2^15
}

case class VoyagerResult(value: String, distance: Float)

/**
 * Voyager reader class for nearest neighbor lookups. Supports looking up neighbors for a vector and
 * returning the string labels and distances associated.
 *
 * @param indexFileName
 *   The path to the `index.hnsw` local or remote file.
 * @param namesFileName
 *   The path to the `names.json` local or remote file.
 * @param spaceType
 *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot (inner
 *   product).
 * @param storageDataType
 *   The Storage type of the vectors at rest. One of Float8, Float32 or E4M3.
 * @param dim
 *   Number of dimensions in vectors.
 */
class VoyagerReader private[voyager] (
  indexFileName: String,
  namesFileName: String,
  spaceType: SpaceType,
  storageDataType: StorageDataType,
  dim: Int
) {
  require(dim > 0, "Vector dimension should be > 0")

  private val index: StringIndex = StringIndex
    .load(indexFileName, namesFileName, spaceType, dim, storageDataType)

  /**
   * Gets maxNumResults nearest neighbors for vector v using ef (where ef is the size of the dynamic
   * list for the nearest neighbors during search).
   */
  def getNearest(v: Array[Float], maxNumResults: Int, ef: Int): Array[VoyagerResult] = {
    val queryResults = index.query(v, maxNumResults, ef)
    queryResults.getNames
      .zip(queryResults.getDistances)
      .map { case (name, distance) =>
        VoyagerResult(name, distance)
      }
  }
}

/**
 * Construction for a VoyagerSide input that leverages a synchronized map to ensure that the reader
 * is only loaded once per [[VoyagerUri]].
 */
private[voyager] class VoyagerSideInput(
  val view: PCollectionView[VoyagerUri],
  distanceMeasure: SpaceType,
  storageType: StorageDataType,
  dim: Int
) extends SideInput[VoyagerReader] {

  import VoyagerSideInput._
  override def get[I, O](context: DoFn[I, O]#ProcessContext): VoyagerReader = {
    val uri = context.sideInput(view)
    VOYAGER_URI_MAP.synchronized {
      if (!VOYAGER_URI_MAP.contains(uri)) {
        VOYAGER_URI_MAP.put(uri, uri.getReader(distanceMeasure, storageType, dim))
      }
      VOYAGER_URI_MAP(uri)
    }
  }
}

private object VoyagerSideInput {
  private val VOYAGER_URI_MAP: mutable.Map[VoyagerUri, VoyagerReader] = mutable.HashMap.empty
}
