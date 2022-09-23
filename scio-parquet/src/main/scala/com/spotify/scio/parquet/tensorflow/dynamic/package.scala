/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.parquet.tensorflow

import com.spotify.scio.parquet.{BeamOutputFile, WriterUtils}
import com.spotify.scio.parquet.tensorflow.dynamic.syntax.AllSyntax
import me.lyh.parquet.tensorflow.{ExampleParquetWriter, Schema}
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.ParquetWriter
import org.tensorflow.proto.example.Example

import java.nio.channels.WritableByteChannel

/**
 * Parquet tensorflow package for dynamic destinations. Import All.
 *
 * {{{
 * import com.spotify.scio.parquet.tensorflow.dynamic._
 * }}}
 */
package object dynamic extends AllSyntax {
  class ParquetExampleSink(
    schema: Schema,
    val compression: CompressionCodecName,
    val conf: SerializableConfiguration
  ) extends FileIO.Sink[Example] {
    private val schemaString: String = schema.toJson()
    private var writer: ParquetWriter[Example] = _
    override def open(channel: WritableByteChannel): Unit = {
      val outputFile = BeamOutputFile.of(channel)
      val schema = Schema.fromJson(schemaString)
      val builder = ExampleParquetWriter.builder(outputFile).withSchema(schema)
      writer = WriterUtils.build(builder, conf.get, compression)
    }
    override def write(element: Example): Unit = writer.write(element)
    override def flush(): Unit = writer.close()
  }
}
