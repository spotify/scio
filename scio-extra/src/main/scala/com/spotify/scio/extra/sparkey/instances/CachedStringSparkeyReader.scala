package com.spotify.scio.extra.sparkey.instances

import com.spotify.scio.util.Cache
import com.spotify.sparkey.SparkeyReader

/**
 * A wrapper around `SparkeyReader` that includes an in-memory Caffeine cache.
 */
class CachedStringSparkeyReader(val sparkey: SparkeyReader, val cache: Cache[String, String])
    extends StringSparkeyReader(sparkey) {
  override def get(key: String): Option[String] =
    Option(cache.get(key, sparkey.getAsString(key)))

  def close(): Unit = {
    sparkey.close()
    cache.invalidateAll()
  }
}
