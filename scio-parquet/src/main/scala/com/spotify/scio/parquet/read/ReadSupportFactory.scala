package com.spotify.scio.parquet.read

import magnolify.parquet.ParquetType
import me.lyh.parquet.tensorflow.ExampleReadSupport
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.api.ReadSupport
import org.tensorflow.proto.example.Example

sealed trait ReadSupportFactory[T] extends Serializable {
  def readSupport: ReadSupport[T]
}

object ReadSupportFactory {
  def typed[T](implicit pt: ParquetType[T]): ReadSupportFactory[T] = new ReadSupportFactory[T] {
    def readSupport: ReadSupport[T] = pt.readSupport
  }

  def avro[T]: ReadSupportFactory[T] = new ReadSupportFactory[T] {
    def readSupport: ReadSupport[T] = new AvroReadSupport
  }

  def example: ReadSupportFactory[Example] = new ReadSupportFactory[Example] {
    def readSupport: ReadSupport[Example] = new ExampleReadSupport()
  }
}
