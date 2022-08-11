package com.spotify.scio.parquet.avro.dynamic.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps.writeDynamic
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.parquet.avro.{ParquetAvroIO, ParquetAvroSink}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

final class DynamicParquetAvroSCollectionOps[T](
  private val self: SCollection[T]
) extends AnyVal {

  /** Save this SCollection of Avro records as a Parquet files written to dynamic destinations. */
  def saveAsDynamicParquetAvroFile(
    path: String,
    schema: Schema = ParquetAvroIO.WriteParam.DefaultSchema,
    numShards: Int = ParquetAvroIO.WriteParam.DefaultNumShards,
    suffix: String = ParquetAvroIO.WriteParam.DefaultSuffix,
    compression: CompressionCodecName = ParquetAvroIO.WriteParam.DefaultCompression,
    conf: Configuration = ParquetAvroIO.WriteParam.DefaultConfiguration,
    tempDirectory: String = ParquetAvroIO.WriteParam.DefaultTempDirectory
  )(
    destinationFn: T => String
  )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Parquet avro file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val cls = ScioUtil.classOf[T]
      val isAssignable = classOf[SpecificRecordBase].isAssignableFrom(cls)
      val writerSchema = if (isAssignable) ReflectData.get().getSchema(cls) else schema
      if (writerSchema == null) throw new IllegalArgumentException("Schema must not be null")
      val sink =
        new ParquetAvroSink[T](writerSchema, compression, new SerializableConfiguration(conf))
      val write = writeDynamic(path, numShards, suffix, destinationFn, tempDirectory).via(sink)
      self.applyInternal(write)
    }
    ClosedTap[Nothing](EmptyTap)
  }
}

trait SCollectionSyntax {
  implicit def dynamicParquetAvroSCollectionOps[T](
    sc: SCollection[T]
  ): DynamicParquetAvroSCollectionOps[T] =
    new DynamicParquetAvroSCollectionOps(sc)
}
