/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.parquet.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.io.Tap
import com.spotify.scio.parquet.{BeamInputFile, ParquetConfiguration}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.beam.sdk.io._
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetInputFormat

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

sealed trait ParquetAvroTap[T <: IndexedRecord] extends Tap[T] {
  def path: String
  def schema: Schema
  def params: ParquetAvroIO.ReadParam[T]

  override def value: Iterator[T] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val xs = FileSystems.`match`(filePattern).metadata().asScala.toList
    val conf = ParquetConfiguration.ofNullable(params.conf)
    Option(params.projection).foreach(AvroReadSupport.setRequestedProjection(conf, _))
    Option(params.predicate).foreach(ParquetInputFormat.setFilterPredicate(conf, _))

    xs.iterator.flatMap { metadata =>
      val reader = AvroParquetReader
        .builder[T](BeamInputFile.of(metadata.resourceId()))
        .withConf(conf)
        .build()

      new Iterator[T] {
        private var current: T = reader.read()

        override def hasNext: Boolean = current != null

        override def next(): T = {
          val prev = current
          current = reader.read()
          prev
        }
      }
    }
  }
}

final case class ParquetGenericRecordTap(
  path: String,
  schema: Schema,
  params: ParquetGenericRecordIO.ReadParam = ParquetGenericRecordIO.ReadParam()
) extends ParquetAvroTap[GenericRecord] {
  override def open(sc: ScioContext): SCollection[GenericRecord] =
    sc.read(ParquetGenericRecordIO(path, schema))(params)
}

final case class ParquetSpecificRecordTap[T <: SpecificRecord: ClassTag](
  path: String,
  params: ParquetSpecificRecordIO.ReadParam[T] = ParquetSpecificRecordIO.ReadParam[T]()
) extends ParquetAvroTap[T] {
  override def schema: Schema = SpecificData.get().getSchema(ScioUtil.classOf[T])
  override def open(sc: ScioContext): SCollection[T] =
    sc.read(ParquetSpecificRecordIO[T](path))(params)
}
