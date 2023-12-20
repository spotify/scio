package com.spotify.scio.extra.sparkey

import java.lang.Math.floorMod
import java.util.UUID
import com.spotify.scio.coders.{BeamCoders, Coder}
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

/** Special version of [[com.spotify.scio.io.ScioIO]] for use with sparkey read methods. */
case class SparkeyIO(path: String) extends TestIO[SparkeyReader] {
  override val tapT: TapT.Aux[SparkeyReader, SparkeyReader] = TapOf[SparkeyReader]
}

/** Special version of [[com.spotify.scio.io.ScioIO]] for use with sparkey write methods. */
case class SparkeyIOOutput[K, V](path: String) extends TestIO[(K, V)] {
  override val tapT: TapT.Aux[(K, V), (K, V)] = TapOf[(K, V)]
}

object SparkeyIO {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val DefaultNumShards: Short = 1
  val DefaultSideInputNumShards: Short = 64
  val DefaultCompressionType: CompressionType = CompressionType.NONE
  val DefaultCompressionBlockSize: Int = 0

  def output[K, V](path: String): SparkeyIOOutput[K, V] = SparkeyIOOutput[K, V](path)

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
    require(numShards > 0, s"numShards must be greater than 0, found $numShards")
    if (compressionType != CompressionType.NONE) {
      require(
        compressionBlockSize > 0,
        s"Compression block size must be > 0 for $compressionType"
      )
    }
    val isUnsharded = numShards == 1
    val rfu = RemoteFileUtil.create(data.context.options)
    val tempLocation = data.context.options.getTempLocation

    // verify that we're not writing to a previously-used output dir
    baseUri.verifyEmpty(rfu)
    // root destination to which all _interim_ results are written,
    // deleted upon successful completion of the write
    val tempPath = s"$tempLocation/sparkey-temp-${UUID.randomUUID}"

    val outputUri = if (isUnsharded) baseUri else SparkeyUri(s"${baseUri.path}/*")
    logger.info(s"Saving as Sparkey with $numShards shards: ${baseUri.path}")

    implicit val coder: Coder[(K, V)] = BeamCoders.getCoder(data)
    implicit val keyCoder: Coder[K] = BeamCoders.getKeyCoder(data)
    implicit val valueCoder: Coder[V] = BeamCoders.getValueCoder(data)

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

      // gather shards that actually have values
      val shardsWithKeys = shards.keys.asSetSingletonSideInput
      // fill in missing shards
      val missingShards = shards.context
        .parallelize((0 until numShards.toInt).map(_.toShort))
        .withSideInputs(shardsWithKeys)
        .flatMap { case (shard, ctx) =>
          val shardExists = ctx(shardsWithKeys).contains(shard)
          if (shardExists) None else Some(shard -> Iterable.empty[(K, V)])
        }
        .toSCollection

      // write files to temporary locations
      val tempShardUris = shards
        .union(missingShards)
        .map { case (shard, xs) =>
          // use a temp uri so that if a bundle fails retries will not fail
          val tempUri = SparkeyUri(s"$tempPath/${UUID.randomUUID}")
          // perform the write to the temp uri
          shard -> writeToSparkey(
            tempUri.sparkeyUriForShard(shard, numShards),
            rfu,
            maxMemoryUsage,
            compressionType,
            compressionBlockSize,
            xs,
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
          logger.info(s"Copying ${items.size} files.")
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
