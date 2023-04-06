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

package com.spotify.scio.extra.sparkey

import com.spotify.scio.extra.sparkey.instances.ShardedSparkeyReader

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import com.spotify.sparkey.extra.ThreadLocalSparkeyReader
import com.spotify.sparkey.{CompressionType, Sparkey, SparkeyReader}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, MatchResult}
import org.apache.beam.sdk.options.PipelineOptions

import scala.jdk.CollectionConverters._

/**
 * Represents the base URI for a Sparkey index and log file, either on the local or a remote file
 * system. For remote file systems, `basePath` should be in the form
 * 'scheme://<bucket>/<path>/<sparkey-prefix>'. For local files, it should be in the form
 * '/<path>/<sparkey-prefix>'. Note that `basePath` must not be a folder or GCS bucket as it is a
 * base path representing two files - <sparkey-prefix>.spi and <sparkey-prefix>.spl.
 */
sealed trait SparkeyUri extends Serializable {
  val basePath: String
  def getReader: SparkeyReader

  private[sparkey] def exists: Boolean
  override def toString: String = basePath
}

private[sparkey] object SparkeyUri {
  def apply(basePath: String, opts: PipelineOptions = null): SparkeyUri =
    if (ScioUtil.isLocalUri(new URI(basePath))) {
      LocalSparkeyUri(basePath)
    } else {
      if (opts != null) {
        RemoteFileUtil.configure(opts)
      }
      RemoteSparkeyUri(basePath)
    }
  def extensions: Seq[String] = Seq(".spi", ".spl")
}

private[sparkey] case class LocalSparkeyUri(basePath: String) extends SparkeyUri {
  override def getReader: SparkeyReader =
    new ThreadLocalSparkeyReader(new File(basePath))
  override private[sparkey] def exists: Boolean =
    SparkeyUri.extensions.map(e => new File(basePath + e)).exists(_.exists)
}

private[sparkey] case class RemoteSparkeyUri(basePath: String) extends SparkeyUri {
  override def getReader: SparkeyReader = {
    val uris = SparkeyUri.extensions.map(e => new URI(basePath + e))
    val paths = RemoteFileUtil.download(uris.asJava).asScala
    new ThreadLocalSparkeyReader(paths.head.toFile)
  }
  override private[sparkey] def exists: Boolean =
    SparkeyUri.extensions
      .exists(e => RemoteFileUtil.remoteExists(new URI(basePath + e)))
}

private[sparkey] class SparkeyWriter(
  val uri: SparkeyUri,
  compressionType: CompressionType,
  compressionBlockSize: Int,
  maxMemoryUsage: Long = -1
) {
  private val localFile = uri match {
    case u: LocalSparkeyUri  => u.basePath
    case _: RemoteSparkeyUri => Files.createTempDirectory("sparkey-").resolve("data").toString
    case _                   => throw new NotImplementedError(s"Unsupported SparkeyUri $uri")
  }

  private lazy val delegate = {
    val file = new File(localFile)
    Files.createDirectories(file.getParentFile.toPath)
    Sparkey.createNew(file, compressionType, compressionBlockSize)
  }

  def put(key: String, value: String): Unit = delegate.put(key, value)

  def put(key: Array[Byte], value: Array[Byte]): Unit = delegate.put(key, value)

  def close(): Unit = {
    delegate.flush()
    if (maxMemoryUsage > 0) {
      delegate.setMaxMemory(maxMemoryUsage)
    }
    delegate.writeHash()
    delegate.close()
    uri match {
      case u: RemoteSparkeyUri =>
        // Copy .spi and .spl to GCS
        SparkeyUri.extensions.foreach { e =>
          val src = Paths.get(localFile + e)
          val dst = new URI(u.basePath + e)
          RemoteFileUtil.upload(src, dst)
        }
      case _ => ()
    }
  }
}

sealed trait ShardedSparkeyUri extends SparkeyUri {
  val basePath: String
  override def getReader: ShardedSparkeyReader

  private[sparkey] def exists: Boolean
  override def toString: String = basePath

  def basePathForShard(shardIndex: Short, numShards: Short): String =
    f"$basePath/part-$shardIndex%05d-of-$numShards%05d"

  def sparkeyUriForShard(shardIndex: Short, numShards: Short): SparkeyUri

  lazy val globExpression: String = s"$basePath/part-*"

  private[sparkey] def basePathsAndCount(
    emptyMatchTreatment: EmptyMatchTreatment
  ): (Seq[String], Short) = {
    val matchResult: MatchResult = FileSystems.`match`(globExpression, emptyMatchTreatment)
    val paths = matchResult.metadata().asScala.map(_.resourceId.toString)

    val indexPaths = paths.filter(_.endsWith(".spi")).sorted

    val allStartParts = indexPaths.map(ShardedSparkeyUri.shardIndexFromPath)
    val allEndParts = indexPaths.map(ShardedSparkeyUri.numShardsFromPath)

    val distinctNumShards = allEndParts.distinct.toList

    distinctNumShards match {
      case Nil => (Seq.empty[String], 0)
      case numShards :: Nil =>
        val numShardFiles = allStartParts.toSet.size
        require(
          numShardFiles <= numShards,
          "Expected the number of Sparkey shards to be less than or equal to the " +
            s"total shard count ($numShards), but found $numShardFiles"
        )

        val basePaths = indexPaths.iterator.map(_.replaceAll("\\.spi$", "")).toSeq

        (basePaths, numShards)
      case _ =>
        throw new InvalidShards(
          s"Expected .spi files to end with the same shard count, got: $distinctNumShards."
        )
    }
  }

  case class InvalidShards(str: String) extends RuntimeException(str)
}

private[sparkey] object ShardedSparkeyUri {
  def apply(basePath: String, options: PipelineOptions = null): ShardedSparkeyUri =
    if (ScioUtil.isLocalUri(new URI(basePath))) {
      LocalShardedSparkeyUri(basePath)
    } else {
      if (options != null) {
        RemoteFileUtil.configure(options)
      }
      RemoteShardedSparkeyUri(basePath)
    }

  private[sparkey] def numShardsFromPath(path: String): Short =
    path.split("-of-").toList.last.split("\\.").head.toShort

  private[sparkey] def shardIndexFromPath(path: String): Short =
    path.split("part-").toList.last.split("-of-").head.toShort

  private[sparkey] def localReadersByShard(
    localBasePaths: Iterable[String]
  ): Map[Short, SparkeyReader] =
    localBasePaths.iterator.map { path =>
      val shardIndex = ShardedSparkeyUri.shardIndexFromPath(path)
      val reader = new ThreadLocalSparkeyReader(new File(path + ".spi"))
      (shardIndex, reader)
    }.toMap
}

private[sparkey] case class LocalShardedSparkeyUri(basePath: String) extends ShardedSparkeyUri {
  override def getReader: ShardedSparkeyReader = {
    val (basePaths, numShards) = basePathsAndCount(EmptyMatchTreatment.DISALLOW)
    new ShardedSparkeyReader(ShardedSparkeyUri.localReadersByShard(basePaths), numShards)
  }

  override private[sparkey] def exists: Boolean =
    basePathsAndCount(EmptyMatchTreatment.ALLOW)._1
      .exists(path => SparkeyUri.extensions.map(e => new File(path + e)).exists(_.exists))

  override def sparkeyUriForShard(shardIndex: Short, numShards: Short): LocalSparkeyUri =
    LocalSparkeyUri(basePathForShard(shardIndex, numShards))
}

private[sparkey] object RemoteShardedSparkeyUri {
  def apply(basePath: String, options: PipelineOptions): RemoteShardedSparkeyUri = {
    RemoteFileUtil.configure(options)
    RemoteShardedSparkeyUri(basePath)
  }
}

private[sparkey] case class RemoteShardedSparkeyUri(basePath: String) extends ShardedSparkeyUri {
  override def getReader: ShardedSparkeyReader = {
    val (basePaths, numShards) = basePathsAndCount(EmptyMatchTreatment.DISALLOW)

    // This logic is copied here so we can download all of the relevant shards in parallel.
    val paths = RemoteFileUtil
      .download(
        basePaths
          .flatMap(shardBasePath =>
            SparkeyUri.extensions.map(extension => new URI(s"$shardBasePath$extension"))
          )
          .toList
          .asJava
      )
      .asScala

    val downloadedBasePaths = paths
      .map(_.toAbsolutePath.toString.replaceAll("\\.sp[il]$", ""))
      .toSet

    new ShardedSparkeyReader(ShardedSparkeyUri.localReadersByShard(downloadedBasePaths), numShards)
  }

  override def sparkeyUriForShard(shardIndex: Short, numShards: Short): RemoteSparkeyUri =
    RemoteSparkeyUri(basePathForShard(shardIndex, numShards))

  override private[sparkey] def exists: Boolean =
    basePathsAndCount(EmptyMatchTreatment.ALLOW)._1
      .exists(path =>
        SparkeyUri.extensions.exists(e => RemoteFileUtil.remoteExists(new URI(path + e)))
      )
}
