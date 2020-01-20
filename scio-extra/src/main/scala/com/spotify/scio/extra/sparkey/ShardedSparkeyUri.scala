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

import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.sparkey.instances.ShardedSparkeyReader
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import com.spotify.sparkey.SparkeyReader
import com.spotify.sparkey.extra.ThreadLocalSparkeyReader
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MatchResult
import org.apache.beam.sdk.options.PipelineOptions

import scala.collection.JavaConverters._

/**
 * Sharded Sparkey support
 */
trait ShardedSparkeyUri extends SparkeyUri {
  val basePath: String
  override def getReader: ShardedSparkeyReader

  private[sparkey] def exists: Boolean
  override def toString: String = basePath

  implicit def coderSparkeyURI: Coder[ShardedSparkeyUri] = Coder.kryo[ShardedSparkeyUri]

  def basePathForShard(shardIndex: Short, numShards: Short): String =
    f"$basePath/part-$shardIndex%05d-of-$numShards%05d"

  def sparkeyUriForShard(shardIndex: Short, numShards: Short): SparkeyUri

  val globExpression = s"$basePath/part-*"

  private[sparkey] def basePathsAndCount(): (Seq[String], Short) = {
    val matchResult: MatchResult = FileSystems.`match`(globExpression)
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
  def apply(basePath: String, options: PipelineOptions): ShardedSparkeyUri =
    if (ScioUtil.isLocalUri(new URI(basePath))) {
      LocalShardedSparkeyUri(basePath)
    } else {
      RemoteShardedSparkeyUri(basePath, RemoteFileUtil.create(options))
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

private case class LocalShardedSparkeyUri(basePath: String) extends ShardedSparkeyUri {
  override def getReader: ShardedSparkeyReader = {
    val (basePaths, numShards) = basePathsAndCount()
    new ShardedSparkeyReader(ShardedSparkeyUri.localReadersByShard(basePaths), numShards)
  }

  override private[sparkey] def exists: Boolean =
    basePathsAndCount()._1
      .exists(path => SparkeyUri.extensions.map(e => new File(path + e)).exists(_.exists))

  override def sparkeyUriForShard(shardIndex: Short, numShards: Short): LocalSparkeyUri =
    LocalSparkeyUri(basePathForShard(shardIndex, numShards))
}

private case class RemoteShardedSparkeyUri(basePath: String, rfu: RemoteFileUtil)
    extends ShardedSparkeyUri {
  override def getReader: ShardedSparkeyReader = {
    val (basePaths, numShards) = basePathsAndCount()

    // This logic is copied here so we can download all of the relevant shards in parallel.
    val paths = rfu
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
    RemoteSparkeyUri(basePathForShard(shardIndex, numShards), rfu)

  override private[sparkey] def exists: Boolean =
    basePathsAndCount()._1
      .exists(path => SparkeyUri.extensions.exists(e => rfu.remoteExists(new URI(path + e))))
}
