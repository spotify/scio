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

package com.spotify.scio.parquet

import com.spotify.scio.parquet.avro.syntax.Syntax
import org.apache.avro.Schema
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetWriter}

import java.nio.channels.WritableByteChannel

/**
 * Main package for Parquet Avro APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.avro._
 * }}}
 */
package object avro extends Syntax {

  /** Alias for `me.lyh.parquet.avro.Projection`. */
  val Projection = me.lyh.parquet.avro.Projection

  /** Alias for `me.lyh.parquet.avro.Predicate`. */
  val Predicate = me.lyh.parquet.avro.Predicate

  class ParquetAvroSink[T](
    val schema: Schema,
    val compression: CompressionCodecName,
    val conf: SerializableConfiguration
  ) extends FileIO.Sink[T] {
    private var writer: ParquetWriter[T] = _
    override def open(channel: WritableByteChannel): Unit = {
      // https://github.com/apache/parquet-mr/tree/master/parquet-hadoop#class-parquetoutputformat
      val rowGroupSize = conf.get.getInt(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
      writer = AvroParquetWriter
        .builder[T](new ParquetOutputFile(channel))
        .withSchema(schema)
        .withCompressionCodec(compression)
        .withConf(conf.get)
        .withRowGroupSize(rowGroupSize)
        .build
    }
    override def write(element: T): Unit = writer.write(element)
    override def flush(): Unit = writer.close()
  }
}
