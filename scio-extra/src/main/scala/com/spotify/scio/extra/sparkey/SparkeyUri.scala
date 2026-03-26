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
import com.spotify.sparkey.Sparkey
import com.spotify.sparkey.SparkeyReader
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, MatchResult, ResourceId}
import org.apache.beam.sdk.options.PipelineOptions

import java.nio.file.Path
import java.util.UUID
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class InvalidNumShardsException(str: String) extends RuntimeException(str)

object SparkeyUri {
  def extensions: Seq[String] = Seq(".spi", ".spl")

  def baseUri(optPath: Option[String], opts: PipelineOptions): SparkeyUri = {
    val tempLocation = opts.getTempLocation
    // the final destination for sparkey files. A temp dir if not permanently persisted.
    val basePath = optPath.getOrElse(s"$tempLocation/sparkey-${UUID.randomUUID}")
    SparkeyUri(basePath)
  }
}

/**
 * Represents the base URI for a Sparkey index and log file, either on the local or a remote file
 * system. For remote file systems, `basePath` should be in the form
 * 'scheme://<bucket>/<path>/<sparkey-prefix>'. For local files, it should be in the form
 * '/<path>/<sparkey-prefix>'. Note that `basePath` must not be a folder or GCS bucket as it is a
 * base path representing two files - <sparkey-prefix>.spi and <sparkey-prefix>.spl.
 */
case class SparkeyUri(path: String) {
  private[sparkey] val isLocal = ScioUtil.isLocalUri(new URI(path))
  private[sparkey] val isSharded = path.endsWith("*")
  private[sparkey] val basePath =
    if (!isSharded) path else path.split("/").dropRight(1).mkString("/")

  private[sparkey] def globExpression: String = {
    require(isSharded, "glob only valid for sharded sparkeys.")
    s"$basePath/part-*"
  }

  private[sparkey] def sparkeyUriForShard(shard: Short, numShards: Short): SparkeyUri =
    SparkeyUri(f"$basePath/part-$shard%05d-of-$numShards%05d")

  private[sparkey] def paths: Seq[ResourceId] = {
    require(!isSharded, "paths only valid for unsharded sparkeys.")
    SparkeyUri.extensions.map(e => FileSystems.matchNewResource(basePath + e, false))
  }

  private def downloadRemoteUris(paths: Seq[String], rfu: RemoteFileUtil): mutable.Buffer[Path] = {
    val downloadPaths = paths.flatMap(bp => SparkeyUri.extensions.map(e => new URI(s"$bp$e")))
    rfu.download(downloadPaths.asJava).asScala
  }

  def getReader(rfu: RemoteFileUtil): SparkeyReader =
    getReader(rfu, SparkeyReadConfig.Default)

  def getReader(rfu: RemoteFileUtil, config: SparkeyReadConfig): SparkeyReader = {
    if (!isSharded) {
      val file =
        if (isLocal) new File(basePath) else downloadRemoteUris(Seq(basePath), rfu).head.toFile
      val totalSize = Sparkey.getIndexFile(file).length() + Sparkey.getLogFile(file).length()
      val useHeap = config.heapBudgetBytes > 0 && totalSize <= config.heapBudgetBytes
      Sparkey.reader().file(file).useHeap(useHeap).open()
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
      new ShardedSparkeyReader(
        ShardedSparkeyUri.localReadersByShard(paths, config.heapBudgetBytes),
        numShards
      )
    }
  }

  def exists(rfu: RemoteFileUtil): Boolean = {
    def localExists(bp: Seq[String]): Boolean =
      bp.exists(p => SparkeyUri.extensions.map(e => new File(p + e)).exists(_.exists))
    def remoteExists(bp: Seq[String]): Boolean =
      bp.exists(p => SparkeyUri.extensions.exists(e => rfu.remoteExists(new URI(p + e))))

    val paths =
      if (!isSharded) Seq(basePath)
      else {
        val (basePaths, _) = ShardedSparkeyUri
          .basePathsAndCount(EmptyMatchTreatment.ALLOW, globExpression)
        basePaths
      }
    if (isLocal) localExists(paths) else remoteExists(paths)
  }
}

private[sparkey] object ShardedSparkeyUri {
  private[sparkey] def shardsFromPath(path: String): (Short, Short) = {
    "part-([0-9]+)-of-([0-9]+)".r
      .findFirstMatchIn(path)
      .map { m =>
        val shard = m.group(1).toShort
        val numShards = m.group(2).toShort
        (shard, numShards)
      }
      .getOrElse {
        throw new IllegalStateException(s"Could not find shard and numShards in [$path]")
      }
  }

  private[sparkey] def localReadersByShard(
    localBasePaths: Iterable[String]
  ): Map[Short, SparkeyReader] =
    localReadersByShard(localBasePaths, 0)

  private[sparkey] def localReadersByShard(
    localBasePaths: Iterable[String],
    heapBudgetBytes: Long
  ): Map[Short, SparkeyReader] = {
    if (heapBudgetBytes <= 0) {
      // No heap budget — all mmap (current behavior)
      localBasePaths.iterator.map { path =>
        val (shardIndex, _) = shardsFromPath(path)
        val reader = Sparkey.open(new File(path + ".spi"))
        (shardIndex, reader)
      }.toMap
    } else {
      // Sort shards largest-first for greedy budget allocation
      val shardsWithSize = localBasePaths
        .map { path =>
          val (shardIndex, _) = shardsFromPath(path)
          val file = new File(path + ".spi")
          val size = Sparkey.getIndexFile(file).length() + Sparkey.getLogFile(file).length()
          (shardIndex, path, size)
        }
        .toSeq
        .sortBy(-_._3)

      case class Acc(
        readers: List[(Short, SparkeyReader)] = Nil,
        remainingBudget: Long = heapBudgetBytes,
        heapShards: Int = 0,
        mmapShards: Int = 0
      )

      val result = shardsWithSize.foldLeft(Acc()) { case (acc, (shardIndex, path, size)) =>
        val file = new File(path + ".spi")
        val useHeap = size <= acc.remainingBudget
        val reader = Sparkey.reader().file(file).useHeap(useHeap).open()
        if (useHeap) {
          acc.copy(
            readers = (shardIndex, reader) :: acc.readers,
            remainingBudget = acc.remainingBudget - size,
            heapShards = acc.heapShards + 1
          )
        } else {
          acc.copy(
            readers = (shardIndex, reader) :: acc.readers,
            mmapShards = acc.mmapShards + 1
          )
        }
      }

      val logger = LoggerFactory.getLogger(classOf[ShardedSparkeyReader])
      val totalSize = shardsWithSize.map(_._3).sum
      val heapBytes = heapBudgetBytes - result.remainingBudget
      logger.info(
        "Opened {} shards: {} on heap ({} bytes), {} mmap. " +
          "Total data: {} bytes, heap budget: {} bytes",
        Array[AnyRef](
          Integer.valueOf(result.heapShards + result.mmapShards),
          Integer.valueOf(result.heapShards),
          java.lang.Long.valueOf(heapBytes),
          Integer.valueOf(result.mmapShards),
          java.lang.Long.valueOf(totalSize),
          java.lang.Long.valueOf(heapBudgetBytes)
        ): _*
      )

      result.readers.toMap
    }
  }

  def basePathsAndCount(
    emptyMatchTreatment: EmptyMatchTreatment,
    globExpression: String
  ): (Seq[String], Short) = {
    val matchResult: MatchResult = FileSystems.`match`(globExpression, emptyMatchTreatment)
    val paths = matchResult.metadata().asScala.map(_.resourceId.toString)

    val indexPaths = paths.filter(_.endsWith(".spi")).sorted
    val shardInfo = indexPaths.map(shardsFromPath)

    val distinctNumShards = shardInfo.map { case (_, numShards) => numShards }.distinct.toList
    distinctNumShards match {
      case Nil              => (Seq.empty[String], 0)
      case numShards :: Nil =>
        val numShardFiles = shardInfo.map { case (shardIdx, _) => shardIdx }.toSet.size
        require(
          numShardFiles <= numShards,
          "Expected the number of Sparkey shards to be less than or equal to the " +
            s"total shard count ($numShards), but found $numShardFiles"
        )

        val basePaths = indexPaths.iterator.map(_.replaceAll("\\.spi$", "")).toSeq

        (basePaths, numShards)
      case _ =>
        throw InvalidNumShardsException(
          s"Expected .spi files to end with the same shard count, got: $distinctNumShards."
        )
    }
  }
}
