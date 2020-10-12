package com.spotify.scio.memcached.syntax

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.memcached.{MemcacheConnectionOptions, MemcachedIOWrite}
import com.spotify.scio.values.SCollection


final class SCollectionMemcacheOps(private val self: SCollection[(String, String)]) {
  def saveAsMemcache(
    memcacheConnectionOptions: MemcacheConnectionOptions
  ): ClosedTap[Nothing] = {
    self.write(MemcachedIOWrite(memcacheConnectionOptions))(MemcachedIOWrite.WriteParam(10))
  }
}

trait SCollectionSyntax {
  implicit  def memcachedIOWrite(colls: SCollection[(String, String)]): SCollectionMemcacheOps = new SCollectionMemcacheOps(colls)
}
