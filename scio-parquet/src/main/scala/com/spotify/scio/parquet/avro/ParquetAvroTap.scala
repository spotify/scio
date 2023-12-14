package com.spotify.scio.parquet.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.parquet.{BeamInputFile, ParquetConfiguration}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io._
import org.apache.parquet.avro.AvroParquetReader

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

final case class ParquetAvroTap[A, T: ClassTag: Coder](
  path: String,
  params: ParquetAvroIO.ReadParam[A, T]
) extends Tap[T] {
  override def value: Iterator[T] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val conf = ParquetConfiguration
      .ofNullable(params.conf)
    conf.setReadSchemas(params)
    conf.setGenericDataSupplierByDefault()

    val xs = FileSystems.`match`(filePattern).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val reader = AvroParquetReader
        .builder[A](BeamInputFile.of(metadata.resourceId()))
        .withConf(conf)
        .build()
      new Iterator[T] {
        private var current: A = reader.read()
        override def hasNext: Boolean = current != null
        override def next(): T = {
          val r = params.projectionFn(current)
          current = reader.read()
          r
        }
      }
    }
  }
  override def open(sc: ScioContext): SCollection[T] =
    sc.read(ParquetAvroIO[T](path))(params)
}
