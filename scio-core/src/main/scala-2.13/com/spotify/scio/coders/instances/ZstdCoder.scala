package com.spotify.scio.coders.instances

import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.{ZstdCoder => BZstdCoder}

import scala.reflect.ClassTag

object ZstdCoder {
  def apply[T: Coder: ClassTag](dict: Array[Byte]): Coder[T] =
    Coder.transform(Coder[T])(tCoder => Coder.beam(BZstdCoder.of(tCoder, dict)))

  def kv[K: Coder, V: Coder](
    keyDict: Array[Byte] = null,
    valueDict: Array[Byte] = null
  ): Coder[(K, V)] =
    Coder.transform(Coder[K]) { kCoder =>
      val bKCoder = Option(keyDict).map(BZstdCoder.of(kCoder, _)).getOrElse(kCoder)
      Coder.transform(Coder[V]) { vCoder =>
        val bVCoder = Option(valueDict).map(BZstdCoder.of(vCoder, _)).getOrElse(vCoder)
        Coder.beam(
          new Tuple2Coder[K, V](bKCoder, bVCoder)
        )
      }
    }
}
