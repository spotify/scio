package com.spotify.scio.parquet.tensorflow.dynamic.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps.writeDynamic
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.parquet.tensorflow.ParquetExampleIO
import com.spotify.scio.parquet.tensorflow.dynamic.ParquetExampleSink
import com.spotify.scio.values.SCollection
import me.lyh.parquet.tensorflow.Schema
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.proto.example.Example

import scala.reflect.ClassTag

final class DynamicParquetExampleSCollectionOps(
  private val self: SCollection[Example]
) extends AnyVal {

  /**
   * Save this SCollection of [[Example]] records as a Parquet files written to dynamic
   * destinations.
   */
  def saveAsDynamicParquetExampleFile(
    path: String,
    schema: Schema,
    numShards: Int = ParquetExampleIO.WriteParam.DefaultNumShards,
    suffix: String = ParquetExampleIO.WriteParam.DefaultSuffix,
    compression: CompressionCodecName = ParquetExampleIO.WriteParam.DefaultCompression,
    conf: Configuration = ParquetExampleIO.WriteParam.DefaultConfiguration,
    tempDirectory: String = ParquetExampleIO.WriteParam.DefaultTempDirectory
  )(
    destinationFn: Example => String
  )(implicit ct: ClassTag[Example], coder: Coder[Example]): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Parquet example file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val sink = new ParquetExampleSink(
        schema,
        compression,
        new SerializableConfiguration(Option(conf).getOrElse(new Configuration()))
      )
      val write = writeDynamic(path, numShards, suffix, destinationFn, tempDirectory).via(sink)
      self.applyInternal(write)
    }
    ClosedTap[Nothing](EmptyTap)
  }
}

trait SCollectionSyntax {
  implicit def dynamicParquetExampleSCollectionOps(
    sc: SCollection[Example]
  ): DynamicParquetExampleSCollectionOps =
    new DynamicParquetExampleSCollectionOps(sc)
}