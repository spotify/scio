package com.spotify.scio.parquet.read

import magnolify.parquet.ParquetType
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.api.ReadSupport

sealed trait ReadSupportFactory[T] extends Serializable {
  def readSupport: ReadSupport[T]
}

object ReadSupportFactory {

  def typed[T: ParquetType]: ReadSupportFactory[T] = new ReadSupportFactory[T] {
    def readSupport: ReadSupport[T] = implicitly[ParquetType[T]].readSupport
  }

  def avro[T]: ReadSupportFactory[T] = new ReadSupportFactory[T] {
    def readSupport: ReadSupport[T] = new AvroReadSupport
  }
}
