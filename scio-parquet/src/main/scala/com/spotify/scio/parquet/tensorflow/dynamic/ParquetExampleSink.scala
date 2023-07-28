package com.spotify.scio.parquet.tensorflow.dynamic

import com.spotify.scio.parquet.tensorflow.ExampleParquetWriter
import com.spotify.scio.parquet.{BeamOutputFile, WriterUtils}
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.ParquetWriter
import org.tensorflow.proto.example.Example
import org.tensorflow.metadata.v0.Schema

import java.nio.channels.WritableByteChannel

class ParquetExampleSink(
  val schema: Schema,
  val compression: CompressionCodecName,
  val conf: SerializableConfiguration
) extends FileIO.Sink[Example] {

  private var writer: ParquetWriter[Example] = _

  override def open(channel: WritableByteChannel): Unit = {
    val outputFile = BeamOutputFile.of(channel)
    val builder = ExampleParquetWriter.builder(outputFile).withSchema(schema)
    writer = WriterUtils.build(builder, conf.get, compression)
  }

  override def write(element: Example): Unit = writer.write(element)

  override def flush(): Unit = writer.close()
}
