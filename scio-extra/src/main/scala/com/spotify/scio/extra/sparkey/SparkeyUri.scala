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

import java.io.File
import java.net.URI
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import com.spotify.scio.extra.sparkey.instances.ShardedSparkeyReader
import com.spotify.sparkey.extra.ThreadLocalSparkeyReader
import com.spotify.sparkey.SparkeyReader
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, MatchResult, ResourceId}

import java.nio.file.Path
import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class InvalidShards(str: String) extends RuntimeException(str)

object SparkeyUri {
  def extensions: Seq[String] = Seq(".spi", ".spl")
  def basePathForShard(basePath: String, shardIndex: Short, numShards: Short): String =
    f"$basePath/part-$shardIndex%05d-of-$numShards%05d"
}

case class SparkeyUri(path: String) {
  private[sparkey] val isLocal = ScioUtil.isLocalUri(new URI(path))
  private val isSharded = path.endsWith("*")
  private[sparkey] val basePath =
    if(!isSharded) path else path.split("/").dropRight(1).mkString("/")

  private[sparkey] def globExpression: String = s"$basePath/part-*"

  private[sparkey] def sparkeyUriForShard(shard: Short, numShards: Short): SparkeyUri =
    SparkeyUri(SparkeyUri.basePathForShard(basePath, shard, numShards))

  private[sparkey] def paths: Seq[ResourceId] = {
    if(isSharded) throw new IllegalStateException("Internal use only for single files.")
    SparkeyUri.extensions.map { e =>
      val url = if(isLocal) new File(basePath + e).toString else new URI(basePath + e).toString
      FileSystems.matchNewResource(url, false)
    }
  }

  private def downloadRemoteUris(paths: Seq[String], rfu: RemoteFileUtil): mutable.Buffer[Path] = {
    val downloadPaths = paths.flatMap { bp => SparkeyUri.extensions.map(e => new URI(s"$bp$e")) }
    rfu.download(downloadPaths.asJava).asScala
  }

  def getReader(rfu: RemoteFileUtil): SparkeyReader = {
    if(!isSharded) {
      val path = if(isLocal) new File(basePath) else {
        downloadRemoteUris(Seq(basePath), rfu).head.toFile
      }
      new ThreadLocalSparkeyReader(path)
    } else {
      val (basePaths, numShards) =
        ShardedSparkeyUri.basePathsAndCount(EmptyMatchTreatment.DISALLOW, globExpression)
      val paths = if (isLocal) basePaths else {
        downloadRemoteUris(basePaths, rfu)
          .map(_.toAbsolutePath.toString.replaceAll("\\.sp[il]$", ""))
          .toSet
      }
      new ShardedSparkeyReader(ShardedSparkeyUri.localReadersByShard(paths), numShards)
    }
  }

  def exists(rfu: RemoteFileUtil): Boolean = {
    if(!isSharded) {
      if(isLocal) {
        SparkeyUri.extensions.map(e => new File(basePath + e)).exists(_.exists)
      } else {
        SparkeyUri.extensions.map(e => new URI(basePath + e)).exists(uri => rfu.remoteExists(uri))
      }
    } else {
      val (basePaths, _) = ShardedSparkeyUri.basePathsAndCount(EmptyMatchTreatment.ALLOW, globExpression)
      if(isLocal) {
        basePaths.exists(p => SparkeyUri.extensions.map(e => new File(p + e)).exists(_.exists))
      } else {
        basePaths.exists(p => SparkeyUri.extensions.exists(e => rfu.remoteExists(new URI(p + e))))
      }
    }

  }
}
//
///**
// * Represents the base URI for a Sparkey index and log file, either on the local or a remote file
// * system. For remote file systems, `basePath` should be in the form
// * 'scheme://<bucket>/<path>/<sparkey-prefix>'. For local files, it should be in the form
// * '/<path>/<sparkey-prefix>'. Note that `basePath` must not be a folder or GCS bucket as it is a
// * base path representing two files - <sparkey-prefix>.spi and <sparkey-prefix>.spl.
// */
//sealed trait SparkeyUri extends Serializable {
//  val basePath: String
//  def getReader: SparkeyReader
//
//  private[sparkey] def exists: Boolean
//  override def toString: String = basePath
//}
//
//trait UnshardedSparkeyUri extends SparkeyUri {
//  private[sparkey] def paths: Seq[String]
//}
//
///** Sharded Sparkey support */
//trait ShardedSparkeyUri extends SparkeyUri {
//  val basePath: String
//  override def getReader: ShardedSparkeyReader
//
//  private[sparkey] def exists: Boolean
//  override def toString: String = basePath
//
//  implicit def coderSparkeyURI: Coder[ShardedSparkeyUri] = Coder.kryo[ShardedSparkeyUri]
//
//  def basePathForShard(shardIndex: Short, numShards: Short): String =
//    f"$basePath/part-$shardIndex%05d-of-$numShards%05d"
//
//  def sparkeyUriForShard(shardIndex: Short, numShards: Short): UnshardedSparkeyUri
//
//  val globExpression: String = s"$basePath/part-*"
//
//  private[sparkey] def basePathsAndCount(emptyMatchTreatment: EmptyMatchTreatment): (Seq[String], Short) =
//    ShardedSparkeyUri.basePathsAndCount(emptyMatchTreatment, globExpression)
//}
//
//private case class LocalSparkeyUri(basePath: String) extends UnshardedSparkeyUri {
//  override def getReader: SparkeyReader = new ThreadLocalSparkeyReader(new File(basePath))
//  private def files: Seq[File] = SparkeyUri.extensions.map(e => new File(basePath + e))
//  override private[sparkey] def paths: Seq[String] = files.map(_.toString)
//  override private[sparkey] def exists: Boolean = files.exists(_.exists)
//}
//
//
//private case class RemoteSparkeyUri(basePath: String, rfu: RemoteFileUtil) extends UnshardedSparkeyUri {
//  override def getReader: SparkeyReader = {
//    val paths = rfu.download(uris.asJava).asScala
//    new ThreadLocalSparkeyReader(paths.head.toFile)
//  }
//  private val uris: Seq[URI] = SparkeyUri.extensions.map(e => new URI(basePath + e))
//  override private[sparkey] def paths: Seq[String] = uris.map(_.toString)
//  override private[sparkey] def exists: Boolean = uris.exists(uri => rfu.remoteExists(uri))
//}
//
//
//private case class LocalShardedSparkeyUri(basePath: String) extends ShardedSparkeyUri {
//  override def getReader: ShardedSparkeyReader = {
//    val (basePaths, numShards) = basePathsAndCount(EmptyMatchTreatment.DISALLOW)
//    new ShardedSparkeyReader(ShardedSparkeyUri.localReadersByShard(basePaths), numShards)
//  }
//
//  override private[sparkey] def exists: Boolean =
//    basePathsAndCount(EmptyMatchTreatment.ALLOW)._1
//      .exists(path => SparkeyUri.extensions.map(e => new File(path + e)).exists(_.exists))
//
//  override def sparkeyUriForShard(shardIndex: Short, numShards: Short): LocalSparkeyUri =
//    LocalSparkeyUri(basePathForShard(shardIndex, numShards))
//}
//
//private case class RemoteShardedSparkeyUri(basePath: String, rfu: RemoteFileUtil)
//  extends ShardedSparkeyUri {
//  override def getReader: ShardedSparkeyReader = {
//    val (basePaths, numShards) = basePathsAndCount(EmptyMatchTreatment.DISALLOW)
//
//    // This logic is copied here so we can download all of the relevant shards in parallel.
//    val paths = rfu
//      .download(
//        basePaths
//          .flatMap(shardBasePath =>
//            SparkeyUri.extensions.map(extension => new URI(s"$shardBasePath$extension"))
//          )
//          .toList
//          .asJava
//      )
//      .asScala
//
//    val downloadedBasePaths = paths
//      .map(_.toAbsolutePath.toString.replaceAll("\\.sp[il]$", ""))
//      .toSet
//
//    new ShardedSparkeyReader(ShardedSparkeyUri.localReadersByShard(downloadedBasePaths), numShards)
//  }
//
//  override def sparkeyUriForShard(shardIndex: Short, numShards: Short): RemoteSparkeyUri =
//    RemoteSparkeyUri(basePathForShard(shardIndex, numShards), rfu)
//
//  override private[sparkey] def exists: Boolean =
//    basePathsAndCount(EmptyMatchTreatment.ALLOW)._1
//      .exists(path => SparkeyUri.extensions.exists(e => rfu.remoteExists(new URI(path + e))))
//}
//
//



//
//private[sparkey] object SparkeyUri {
//  def apply(basePath: String, opts: PipelineOptions): UnshardedSparkeyUri = {
//    val isLocal = ScioUtil.isLocalUri(new URI(basePath))
//    if(isLocal) LocalSparkeyUri(basePath) else RemoteSparkeyUri(basePath, opts)
//  }
//
//  def extensions: Seq[String] = Seq(".spi", ".spl")
//
//  implicit def coderSparkeyURI: Coder[SparkeyUri] = Coder.kryo[SparkeyUri]
//}
//
//
//private object RemoteSparkeyUri {
//  def apply(basePath: String, options: PipelineOptions): RemoteSparkeyUri =
//    RemoteSparkeyUri(basePath, RemoteFileUtil.create(options))
//}
//




private[sparkey] object ShardedSparkeyUri {
//  def apply(basePath: String, options: PipelineOptions): ShardedSparkeyUri =
//    ShardedSparkeyUri(basePath, RemoteFileUtil.create(options))
//
//  def apply(basePath: String, rfu: RemoteFileUtil): ShardedSparkeyUri =
//    if (ScioUtil.isLocalUri(new URI(basePath))) {
//      LocalShardedSparkeyUri(basePath)
//    } else {
//      RemoteShardedSparkeyUri(basePath, rfu)
//    }

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


  def basePathsAndCount(
                         emptyMatchTreatment: EmptyMatchTreatment,
                         globExpression: String
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
}
