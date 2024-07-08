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

package com.spotify.scio.extra.sparkey

import java.lang.Math.floorMod
import java.util.UUID
import com.spotify.scio.extra.sparkey.instances._
import com.spotify.scio.io.{TapOf, TapT, TestIO}
import com.spotify.scio.util.RemoteFileUtil
import com.spotify.scio.values.SCollection
import com.spotify.sparkey.{CompressionType, SparkeyReader}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, ResourceId}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Special version of [[com.spotify.scio.io.ScioIO]] for use with sparkey methods. */
private[sparkey] case class SparkeyTestIO[T](path: String) extends TestIO[T] {
  override val tapT: TapT.Aux[T, T] = TapOf[T]
}

object SparkeyIO {
  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  val DefaultNumShards: Short = 1
  val DefaultSideInputNumShards: Short = 64
  val DefaultCompressionType: CompressionType = CompressionType.NONE
  val DefaultCompressionBlockSize: Int = 0

  def apply(path: String): SparkeyTestIO[SparkeyReader] = SparkeyTestIO[SparkeyReader](path)
  def output[K, V](path: String): SparkeyTestIO[(K, V)] = SparkeyTestIO[(K, V)](path)

  private def writeToSparkey[K, V](
    uri: SparkeyUri,
    rfu: RemoteFileUtil,
    maxMemoryUsage: Long,
    compressionType: CompressionType,
    compressionBlockSize: Int,
    elements: Iterable[(K, V)],
    w: SparkeyWritable[K, V]
  ): SparkeyUri = {
    val writer =
      new SparkeyWriter(uri, rfu, compressionType, compressionBlockSize, maxMemoryUsage)
    val it = elements.iterator
    while (it.hasNext) {
      val kv = it.next()
      w.put(writer, kv._1, kv._2)
    }
    writer.close()
    uri
  }

  /** @param baseUri The final destination for sparkey files */
  private[sparkey] def writeSparkey[K, V](
    baseUri: SparkeyUri,
    writable: SparkeyWritable[K, V],
    data: SCollection[(K, V)],
    maxMemoryUsage: Long,
    numShards: Short,
    compressionType: CompressionType,
    compressionBlockSize: Int
  ): SCollection[SparkeyUri] = {
    require(
      !baseUri.isSharded,
      s"path to which sparkey will be saved must not include a `*` wildcard."
    )
    require(numShards > 0, s"numShards must be greater than 0, found $numShards")
    if (compressionType != CompressionType.NONE) {
      require(
        compressionBlockSize > 0,
        s"Compression block size must be > 0 for $compressionType"
      )
    }
    val sc = data.context
    val isUnsharded = numShards == 1
    val rfu = RemoteFileUtil.create(sc.options)
    val tempLocation = sc.options.getTempLocation

    // verify that we're not writing to a previously-used output dir
    List(baseUri, SparkeyUri(s"${baseUri.path}/*")).foreach { uri =>
      require(!uri.exists(rfu), s"Sparkey URI ${uri.path} already exists")
    }
    // root destination to which all _interim_ results are written,
    // deleted upon successful completion of the write
    val tempPath = s"$tempLocation/sparkey-temp-${UUID.randomUUID}"

    val outputUri = if (isUnsharded) baseUri else SparkeyUri(s"${baseUri.path}/*")
    logger.info(s"Saving as Sparkey with $numShards shards: ${baseUri.path}")

    def resourcesForPattern(pattern: String): mutable.Buffer[ResourceId] =
      FileSystems
        .`match`(pattern, EmptyMatchTreatment.ALLOW)
        .metadata()
        .asScala
        .map(_.resourceId())

    data.transform { collection =>
      // shard by key hash
      val shards = collection
        .groupBy { case (k, _) => floorMod(writable.shardHash(k), numShards.toInt).toShort }

      // all shards
      val allShards = sc
        .parallelize(0 until numShards.toInt)
        .map(_.toShort -> ())

      // write files to temporary locations
      val tempShardUris = shards
        .hashFullOuterJoin(allShards)
        .map { case (shard, (xs, _)) =>
          // use a temp uri so that if a bundle fails retries will not fail
          val tempUri = SparkeyUri(s"$tempPath/${UUID.randomUUID}")
          // perform the write to the temp uri
          shard -> writeToSparkey(
            tempUri.sparkeyUriForShard(shard, numShards),
            rfu,
            maxMemoryUsage,
            compressionType,
            compressionBlockSize,
            xs.getOrElse(Iterable.empty),
            writable
          )
        }

      // TODO WriteFiles inserts a reshuffle here for unclear reasons

      tempShardUris.reifyAsListInGlobalWindow
        .map { seq =>
          val items = seq.toList

          // accumulate source files and destination files
          val (srcPaths, dstPaths) = items
            .foldLeft((List.empty[ResourceId], List.empty[ResourceId])) {
              case ((srcs, dsts), (shard, uri)) =>
                if (isUnsharded && shard != 0)
                  throw new IllegalArgumentException(s"numShards=1 but got shard=$shard")
                // assumes paths always returns things in the same order 🙃
                val dstUri =
                  if (isUnsharded) baseUri else baseUri.sparkeyUriForShard(shard, numShards)

                val srcResources = srcs ++ uri.paths
                val dstResources = dsts ++ dstUri.paths

                (srcResources, dstResources)
            }

          // rename source files to dest files
          logger.info(s"Copying ${items.size} files from temp to final GCS destination.")
          // per FileBasedSink.java#783 ignore errors as files may have previously been deleted
          FileSystems.rename(
            srcPaths.asJava,
            dstPaths.asJava,
            StandardMoveOptions.IGNORE_MISSING_FILES,
            StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS
          )

          // cleanup orphan files per FileBasedSink.removeTemporaryFiles
          val orphanTempFiles = resourcesForPattern(s"${tempPath}/*")
          orphanTempFiles.foreach { r =>
            logger.warn("Will also remove unknown temporary file {}.", r)
          }
          FileSystems.delete(orphanTempFiles.asJava, StandardMoveOptions.IGNORE_MISSING_FILES)
          // clean up temp dir, can fail, but failure is to be ignored per FileBasedSink
          val tempPathResource = resourcesForPattern(tempPath)
          try {
            FileSystems.delete(tempPathResource.asJava, StandardMoveOptions.IGNORE_MISSING_FILES)
          } catch {
            case _: Exception =>
              logger.warn("Failed to remove temporary directory: [{}].", tempPath)
          }

          outputUri
        }
    }
  }
}
