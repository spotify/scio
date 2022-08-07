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
}
