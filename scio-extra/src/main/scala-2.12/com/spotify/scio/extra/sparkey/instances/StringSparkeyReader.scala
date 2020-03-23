package com.spotify.scio.extra.sparkey.instances

import com.spotify.sparkey.SparkeyReader

import scala.collection.JavaConverters._

/** Enhanced version of `SparkeyReader` that mimics a `Map`. */
class StringSparkeyReader(self: SparkeyReader) extends Map[String, String] {
  override def get(key: String): Option[String] =
    Option(self.getAsString(key))
  override def iterator: Iterator[(String, String)] =
    self.iterator.asScala.map(e => (e.getKeyAsString, e.getValueAsString))

  override def +[B1 >: String](kv: (String, B1)): Map[String, B1] =
    throw new NotImplementedError("Sparkey-backed map; operation not supported.")
  override def -(key: String): Map[String, String] =
    throw new NotImplementedError("Sparkey-backed map; operation not supported.")
}
