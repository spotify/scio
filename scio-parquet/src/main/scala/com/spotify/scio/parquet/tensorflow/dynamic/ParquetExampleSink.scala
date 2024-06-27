/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.parquet.tensorflow.dynamic

import com.spotify.parquet.tensorflow.TensorflowExampleParquetWriter
import com.spotify.scio.parquet.{BeamOutputFile, WriterUtils}
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.ParquetWriter
import org.tensorflow.proto.example.Example
import org.tensorflow.metadata.v0.Schema

import java.nio.channels.WritableByteChannel
import scala.jdk.CollectionConverters._

class ParquetExampleSink(
  val schema: Schema,
  val compression: CompressionCodecName,
  val conf: SerializableConfiguration,
  val metadata: Map[String, String]
) extends FileIO.Sink[Example] {

  private var writer: ParquetWriter[Example] = _

  override def open(channel: WritableByteChannel): Unit = {
    val outputFile = BeamOutputFile.of(channel)
    val builder = TensorflowExampleParquetWriter.builder(outputFile).withSchema(schema)
    writer = WriterUtils.build(builder, conf.get, compression, metadata.asJava)
  }

  override def write(element: Example): Unit = writer.write(element)

  override def flush(): Unit = writer.close()
}
