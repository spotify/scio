package com.spotify.scio.coders.extra.sparkey

import com.spotify.scio.coders.{Coder, CoderGrammar}
import com.spotify.sparkey.{SparkeyReader, SparkeyWriter}

import scala.reflect.ClassTag

trait SparkeyCoders extends CoderGrammar {
  implicit def sparkeyReaderCoder[T <: SparkeyReader: ClassTag]: Coder[T] = kryo[T]
  implicit def sparkeyWriterCoder[T <: SparkeyWriter: ClassTag]: Coder[T] = kryo[T]
}

private[coders] object SparkeyCoders extends SparkeyCoders
