package com.spotify.scio.parquet.avro

import com.spotify.scio.parquet.ParquetOutputFile
import org.apache.avro.Schema
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetWriter}

import java.nio.channels.WritableByteChannel

class ParquetAvroSink[T](
  schema: Schema,
  val compression: CompressionCodecName,
  val conf: SerializableConfiguration
) extends FileIO.Sink[T] {
  private val schemaString = schema.toString
  private var writer: ParquetWriter[T] = _

  override def open(channel: WritableByteChannel): Unit = {
    val schema = new Schema.Parser().parse(schemaString)
    // https://github.com/apache/parquet-mr/tree/master/parquet-hadoop#class-parquetoutputformat
    val rowGroupSize =
      conf.get.getInt(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
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
