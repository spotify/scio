package com.spotify.scio.coders.instances
import java.io.{InputStream, OutputStream}

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.AtomicCoder

trait GuavaCoders {

  /**
   * Beam Coder for [[gBloomFilter]]
   */
  implicit def guavaBloomFilterCoder[A](implicit fa: Funnel[A]): Coder[gBloomFilter[A]] =
    Coder.beam(
      new AtomicCoder[gBloomFilter[A]] {
        override def encode(value: gBloomFilter[A], outStream: OutputStream): Unit =
          value.writeTo(outStream)
        override def decode(inStream: InputStream): gBloomFilter[A] =
          gBloomFilter.readFrom[A](inStream, fa)
      }
    )
}
