package com.spotify.scio.memcached

import java.nio.charset.StandardCharsets

import com.spotify.folsom.{MemcacheClient, MemcacheClientBuilder}
import com.spotify.scio.ScioContext
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.values.KV

case class MemcacheConnectionOptions(hostname: String,
                                     port: Int,
                                      ttl: Int,
                                      flushDelay: Int)
object MemcacheConnectionOptions{
  def apply(hostname: String, port: Int, ttl: Int, flushDelay: Int): MemcacheConnectionOptions =
    new MemcacheConnectionOptions(hostname, port, ttl, flushDelay)

  def connect(memcacheConnectionOptions:MemcacheConnectionOptions):MemcacheClient[String] = {
    MemcacheClientBuilder.newStringClient()
      .withAddress(memcacheConnectionOptions.hostname)
      .withKeyCharset(StandardCharsets.UTF_8)
      .connectAscii()
  }
}

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

    val sink = MemcacheIOTransform.write(memcacheConnectionOptions)
    data.map{ case (k, v) => KV.of(k,v)}.applyInternal(sink)

    EmptyTap
  }
}

object MemcachedIOWrite {
  object WriteParam {
    private[memcached] val DefaultConnectWaitSeconds = 10
  }

  final case class WriteParam private (ttl: Int)

}
