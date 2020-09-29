package com.spotify.scio.coders.instances

import java.io.{InputStream, OutputStream}

import com.google.common.hash.BloomFilter
import com.spotify.scio.coders.Coder
import com.google.common.{hash => g}
import org.apache.beam.sdk.coders.AtomicCoder

class GuavaBloomFilterCoder[T](implicit val funnel: g.Funnel[T])
    extends AtomicCoder[g.BloomFilter[T]] {
  override def encode(value: BloomFilter[T], outStream: OutputStream): Unit =
    value.writeTo(outStream)
  override def decode(inStream: InputStream): BloomFilter[T] =
    BloomFilter.readFrom[T](inStream, funnel)
}

trait GuavaCoders {
  implicit def guavaBFCoder[T](implicit x: g.Funnel[T]): Coder[g.BloomFilter[T]] =
    Coder.beam(new GuavaBloomFilterCoder[T])
}
