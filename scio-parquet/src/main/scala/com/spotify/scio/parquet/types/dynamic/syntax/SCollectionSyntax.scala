package com.spotify.scio.parquet.types.dynamic.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps.writeDynamic
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.parquet.types.{ParquetTypeIO, ParquetTypeSink}
import com.spotify.scio.values.SCollection
import magnolify.parquet.ParquetType
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

final class DynamicParquetTypeSCollectionOps[T](
  private val self: SCollection[T]
) extends AnyVal {
    /**
     * Save this SCollection of records as a Parquet files written to dynamic destinations.
     */
    def saveAsDynamicTypedParquetFile(
      path: String,
      numShards: Int = ParquetTypeIO.WriteParam.DefaultNumShards,
      suffix: String = ParquetTypeIO.WriteParam.DefaultSuffix,
      compression: CompressionCodecName = ParquetTypeIO.WriteParam.DefaultCompression,
      conf: Configuration = ParquetTypeIO.WriteParam.DefaultConfiguration,
      tempDirectory: String = ParquetTypeIO.WriteParam.DefaultTempDirectory
    )(
      destinationFn: T => String
    )(implicit ct: ClassTag[T], coder: Coder[T], pt: ParquetType[T]): ClosedTap[Nothing] = {
      if (self.context.isTest) {
        throw new NotImplementedError("Typed parquet file with dynamic destinations cannot be used in a test context")
      } else {
        val sink = new ParquetTypeSink[T](compression, new SerializableConfiguration(conf))
        val write = writeDynamic(path, numShards, suffix, destinationFn, tempDirectory).via(sink)
        self.applyInternal(write)
      }
      ClosedTap[Nothing](EmptyTap)
    }
}

trait SCollectionSyntax {
  implicit def dynamicParquetTypeSCollectionOps[T](
    sc: SCollection[T]
  ): DynamicParquetTypeSCollectionOps[T] =
    new DynamicParquetTypeSCollectionOps(sc)
}
