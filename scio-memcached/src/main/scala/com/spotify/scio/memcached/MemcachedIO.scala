package com.spotify.scio.memcached

import com.spotify.scio.ScioContext
import com.spotify.scio.io._
import com.spotify.scio.memcache.{MemcacheConnectionConfiguration, MemcacheIO}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.values.KV

case class MemcacheConnectionOptions(hostname: String,
                                     port: Int,
                                      ttl: Int,
                                      flushDelay: Int)

sealed trait MemcachedIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
}

final case class MemcachedIOWrite(memcacheConnectionOptions: MemcacheConnectionOptions) extends MemcachedIO[(String, String)] {
  override type ReadP = Unit
  override type WriteP = MemcachedIOWrite.WriteParam

  override def tap(read: ReadP): Tap[Nothing] = EmptyTap

  override protected def read(sc: ScioContext, params: ReadP): SCollection[(String, String)] =
    throw new UnsupportedOperationException("cannot write to Memcached yet")

  override protected def write(
    data: SCollection[(String, String)],
    params: WriteP
  ): Tap[Nothing] = {

    val connectionconConfig = MemcachedIOWrite.toConnectionConfig(memcacheConnectionOptions)
    val sink: MemcacheIO.Write = MemcacheIO.write().withMemcacheConnectionConfiguration(connectionconConfig)
    data.map{ case (k, v) => KV.of(k,v)}.applyInternal(sink)

    EmptyTap
  }
}

object MemcachedIOWrite {
  def toConnectionConfig(mco: MemcacheConnectionOptions): MemcacheConnectionConfiguration =
    MemcacheConnectionConfiguration.create(mco.hostname, mco.port, mco.ttl,mco.flushDelay)

  object WriteParam {
    private[memcached] val DefaultConnectWaitSeconds = 10
  }

  final case class WriteParam private (ttl: Int)

}
