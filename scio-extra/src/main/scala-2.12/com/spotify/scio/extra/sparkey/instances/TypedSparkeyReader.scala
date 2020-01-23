package com.spotify.scio.extra.sparkey.instances

import java.nio.charset.Charset

import com.spotify.scio.util.Cache
import com.spotify.sparkey.SparkeyReader

import scala.collection.JavaConverters._

/**
 * A wrapper around `SparkeyReader` that includes both a decoder (to map from each byte array
 * to a JVM type) and an optional in-memory cache.
 */
class TypedSparkeyReader[T](
  val sparkey: SparkeyReader,
  val decoder: Array[Byte] => T,
  val cache: Cache[String, T] = Cache.noOp
) extends Map[String, T] {
  private def stringKeyToBytes(key: String): Array[Byte] = key.getBytes(Charset.defaultCharset())

  private def loadValueFromSparkey(key: String): T = {
    val value = sparkey.getAsByteArray(stringKeyToBytes(key))
    if (value == null) {
      null.asInstanceOf[T]
    } else {
      decoder(value)
    }
  }

  override def get(key: String): Option[T] =
    Option(cache.get(key, loadValueFromSparkey(key)))

  override def iterator: Iterator[(String, T)] =
    sparkey.iterator.asScala.map { e =>
      val key = e.getKeyAsString
      val value = cache.get(key).getOrElse(decoder(e.getValue))
      (key, value)
    }

  override def +[B1 >: T](kv: (String, B1)): Map[String, B1] =
    throw new NotImplementedError("Sparkey-backed map; operation not supported.")
  override def -(key: String): Map[String, T] =
    throw new NotImplementedError("Sparkey-backed map; operation not supported.")

  def close(): Unit = {
    sparkey.close()
    cache.invalidateAll()
  }
}
