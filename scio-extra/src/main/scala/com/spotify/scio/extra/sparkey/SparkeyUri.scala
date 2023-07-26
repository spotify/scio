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
    if (!isSharded) path else path.split("/").dropRight(1).mkString("/")

  private[sparkey] def globExpression: String = s"$basePath/part-*"

  private[sparkey] def sparkeyUriForShard(shard: Short, numShards: Short): SparkeyUri =
    SparkeyUri(SparkeyUri.basePathForShard(basePath, shard, numShards))

  private[sparkey] def paths: Seq[ResourceId] = {
    if (isSharded) throw new IllegalStateException("Internal use only for single files.")
    SparkeyUri.extensions.map { e =>
      val url = if (isLocal) new File(basePath + e).toString else new URI(basePath + e).toString
      FileSystems.matchNewResource(url, false)
    }
  }

  private def downloadRemoteUris(paths: Seq[String], rfu: RemoteFileUtil): mutable.Buffer[Path] = {
    val downloadPaths = paths.flatMap(bp => SparkeyUri.extensions.map(e => new URI(s"$bp$e")))
    rfu.download(downloadPaths.asJava).asScala
  }

  def getReader(rfu: RemoteFileUtil): SparkeyReader = {
    if (!isSharded) {
      val path =
        if (isLocal) new File(basePath)
        else {
          downloadRemoteUris(Seq(basePath), rfu).head.toFile
        }
      new ThreadLocalSparkeyReader(path)
    } else {
      val (basePaths, numShards) =
        ShardedSparkeyUri.basePathsAndCount(EmptyMatchTreatment.DISALLOW, globExpression)
      val paths =
        if (isLocal) basePaths
        else {
          downloadRemoteUris(basePaths, rfu)
            .map(_.toAbsolutePath.toString.replaceAll("\\.sp[il]$", ""))
            .toSet
        }
      new ShardedSparkeyReader(ShardedSparkeyUri.localReadersByShard(paths), numShards)
    }
  }

  def exists(rfu: RemoteFileUtil): Boolean = {
    if (!isSharded) {
      if (isLocal) {
        SparkeyUri.extensions.map(e => new File(basePath + e)).exists(_.exists)
      } else {
        SparkeyUri.extensions.map(e => new URI(basePath + e)).exists(uri => rfu.remoteExists(uri))
      }
    } else {
      val (basePaths, _) =
        ShardedSparkeyUri.basePathsAndCount(EmptyMatchTreatment.ALLOW, globExpression)
      if (isLocal) {
        basePaths.exists(p => SparkeyUri.extensions.map(e => new File(p + e)).exists(_.exists))
      } else {
        basePaths.exists(p => SparkeyUri.extensions.exists(e => rfu.remoteExists(new URI(p + e))))
      }
    }

  }
}

private[sparkey] object ShardedSparkeyUri {
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
