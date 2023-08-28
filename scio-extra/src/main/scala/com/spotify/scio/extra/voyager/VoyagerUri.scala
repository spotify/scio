package com.spotify.scio.extra.voyager

import com.spotify.scio.coders.Coder
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}

import java.nio.charset.StandardCharsets
import com.spotify.voyager.jni.{Index, StringIndex}
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import org.apache.beam.sdk.options.PipelineOptions

import scala.jdk.CollectionConverters
import scala.collection.mutable
import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

trait VoyagerUri extends Serializable {
  val path: String
  private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ): VoyagerReader
  private[voyager] def saveAndClose(voyagerWriter: VoyagerWriter): Unit
  private[voyager] def exists: Boolean
}

private[voyager] object VoyagerUri {
  def apply(path: String, opts: PipelineOptions): VoyagerUri =
    if (ScioUtil.isLocalUri(new URI(path))) {
      new LocalVoyagerUri(path)
    } else {
      new RemoteVoyagerUri(path, opts)
    }
  def files: Seq[String] = Seq("index.hnsw", "names.json")
  implicit val voyagerUriCoder: Coder[VoyagerUri] = Coder.kryo[VoyagerUri]
}

private class LocalVoyagerUri(val path: String) extends VoyagerUri {
  override private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ): VoyagerReader =
    new VoyagerReader(path, distanceMeasure, storageType, dim)

  override private[voyager] def saveAndClose(w: VoyagerWriter): Unit = {
    val indexPath = path + "/index.hnsw"
    val namesPath = path + "/names.json"
    w.save(indexPath, namesPath)
    w.close()
  }

  override private[voyager] def exists: Boolean =
    VoyagerUri.files.map(f => new File(path + "/" + f)).exists(_.exists())
}

private class RemoteVoyagerUri(val path: String, options: PipelineOptions) extends VoyagerUri {
  private[this] val rfu: RemoteFileUtil = RemoteFileUtil.create(options)
  override private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ): VoyagerReader = {
    val localPath = rfu.download(new URI(path))
    new VoyagerReader(localPath.toString, distanceMeasure, storageType, dim)
  }

  override private[voyager] def saveAndClose(w: VoyagerWriter): Unit = {
    val indexPath = path + "/index.hnsw"
    val namesPath = path + "/names.json"
    w.save(indexPath, namesPath)
    w.close()
  }

  override private[voyager] def exists: Boolean =
    VoyagerUri.files.exists(f => rfu.remoteExists(new URI(path + "/" + f)))
}

private[voyager] class VoyagerWriter(
  distanceMeasure: VoyagerDistanceMeasure,
  storageType: VoyagerStorageType,
  dim: Int,
  ef: Long = 200L,
  m: Long = 16L
) {

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

  def save(indexFileName: String, namesFileName: String): Unit = {
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
