package com.spotify.scio.memcached.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.memcached.{MemcacheConnectionOptions, MemcachedIOWrite}
import com.spotify.scio.values.SCollection

final class ScioContextOps(private val sc: ScioContext) extends AnyVal {
  def memecahe(
    memcacheConnectionOptions: MemcacheConnectionOptions
  ): SCollection[(String, String)] =
    sc.read(MemcachedIOWrite(memcacheConnectionOptions))
}
trait ScioContextSyntax {
  implicit  def memcacheScioConextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
