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

import com.spotify.scio.coders.Coder
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}

import java.nio.charset.StandardCharsets
import com.spotify.voyager.jni.Index
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import org.apache.beam.sdk.options.PipelineOptions
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters
import scala.collection.mutable
import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}

trait VoyagerUri extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)
  val indexPath: String
  val namesPath: String
  private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ): VoyagerReader
  private[voyager] def saveAndClose(voyagerWriter: VoyagerWriter): Unit
  private[voyager] def exists: Boolean
}

private[voyager] object VoyagerUri {
  def apply(indexPath: String, namesPath: String = null, opts: PipelineOptions): VoyagerUri = {
    val actualNamesPath = Option(namesPath).getOrElse(indexPath)
    if (ScioUtil.isLocalUri(new URI(indexPath))) {
      new LocalVoyagerUri(indexPath, actualNamesPath)
    } else {
      new RemoteVoyagerUri(indexPath, actualNamesPath, opts)
    }
  }

  def files: Seq[String] = Seq("index.hnsw", "names.json")
  implicit val voyagerUriCoder: Coder[VoyagerUri] = Coder.kryo[VoyagerUri]
}

private class LocalVoyagerUri(val indexPath: String, val namesPath: String) extends VoyagerUri {
  override private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ): VoyagerReader = {

    val indexFileName: String = indexPath + "/index.hnsw"
    val namesFileName: String = namesPath + "/names.json"
    new VoyagerReader(indexFileName, namesFileName, distanceMeasure, storageType, dim)
  }

  override private[voyager] def saveAndClose(w: VoyagerWriter): Unit = {
    w.save(indexPath, namesPath)
    w.close()
  }

  override private[voyager] def exists: Boolean =
    new File(indexPath + "/index.hnsw").exists() && new File(namesPath + "/names.json").exists()
}

private class RemoteVoyagerUri(
  val indexPath: String,
  val namesPath: String,
  options: PipelineOptions
) extends VoyagerUri {
  private[this] val rfu: RemoteFileUtil = RemoteFileUtil.create(options)
  override private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ): VoyagerReader = {
    val indexFileName: String = rfu.download(new URI(indexPath + "/index.hnsw")).toString
    val namesFileName: String = rfu.download(new URI(namesPath + "/names.json")).toString
    new VoyagerReader(indexFileName, namesFileName, distanceMeasure, storageType, dim)
  }

  override private[voyager] def saveAndClose(w: VoyagerWriter): Unit = {
    val tempPath: Path = Files.createTempDirectory("")
    logger.info(s"temp path: $tempPath")
    w.save(tempPath.toString, tempPath.toString)
    w.close()

    val tempIndexPath = tempPath.resolve("index.hnsw")
    rfu.upload(Paths.get(tempIndexPath.toString), new URI(indexPath + "/index.hnsw"))
    val tempNamesPath = tempPath.resolve("names.json")
    rfu.upload(Paths.get(tempNamesPath.toString), new URI(namesPath + "/names.json"))

    Files.delete(tempIndexPath)
    Files.delete(tempNamesPath)
  }

  override private[voyager] def exists: Boolean =
    rfu.remoteExists(new URI(indexPath + "/index.hnsw")) && rfu.remoteExists(
      new URI(namesPath + "/names.json")
    )
}

private[voyager] class VoyagerWriter(
  distanceMeasure: VoyagerDistanceMeasure,
  storageType: VoyagerStorageType,
  dim: Int,
  ef: Long = 200L,
  m: Long = 16L
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Chunk size experiments - <chunk_size>, <num_chunks>
  // 4096, 6062: 2022-11-16 14:07:07.358 -> 2022-11-16 16:50:59.109 = 2hr 50min.  1.68s per chunk
  // 32786, 758: 2022-11-16 15:37:11.374 -> 2022-11-16 16:50:29.396 = 1hr 13min.  5.77s per chunk
  // 131072, 190: 2022-11-17 13:38:08.421 -> 2022-11-17 15:42:39.929 = 2hr 6min.  39.79s per chunk
  private val chunkSize = 32786 // 2^15
  private val randomSeed = 1L
  private[this] val namesOutput = mutable.ListBuffer.empty[String]

  private[this] val index: Index = {
    val spaceType = distanceMeasure match {
      case Euclidean => SpaceType.Euclidean
      case Cosine    => SpaceType.Cosine
      case Dot       => SpaceType.InnerProduct
    }

    val storageDataType = storageType match {
      case Float8  => StorageDataType.Float8
      case Float32 => StorageDataType.Float32
      case E4M3    => StorageDataType.E4M3
    }
    new Index(spaceType, dim, m, ef, randomSeed, chunkSize, storageDataType)
  }

  def write(vectors: Iterable[(String, Array[Float])]): Unit = {
    var batchNum = 1

    val nameVectorIndexIterator = vectors.iterator.zipWithIndex
      .map { case ((name, vector), idx) =>
        (name, vector, idx.longValue())
      }

    while (nameVectorIndexIterator.hasNext) {
      val (nameArray, vectorArray, indexArray) = nameVectorIndexIterator
        .take(chunkSize)
        .toArray
        .unzip3

      index.addItems(vectorArray, indexArray, -1)

      batchNum += 1
      namesOutput ++= nameArray
    }

    ()
  }

  def save(indexPath: String, namesPath: String): Unit = {
    val indexFileName: String = indexPath + "/index.hnsw"
    val namesFileName: String = namesPath + "/names.json"
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
