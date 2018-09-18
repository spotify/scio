/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.coders

import org.apache.beam.sdk.{coders => bcoders}
import org.apache.beam.sdk.coders.{Coder => _, _}
import org.apache.beam.sdk.values.KV
import java.nio.ByteBuffer
import java.io.{InputStream, OutputStream}

final class ByteBufferCoder private[coders] () extends AtomicCoder[ByteBuffer] {
  val bac = ByteArrayCoder.of()
  def encode(value: ByteBuffer, os: OutputStream): Unit = {
    val array =
      if (value.hasArray) {
        value.array()
      } else {
        value.clear()
        val a = new Array[Byte](value.capacity())
        value.get(a, 0, a.length)
        a
      }
    bac.encode(array, os)
  }

  def decode(is: InputStream): ByteBuffer = {
    val bytes = bac.decode(is)
    ByteBuffer.wrap(bytes)
  }
}

//
// Java Coders
//
trait JavaCoders {

  implicit def uriCoder: Coder[java.net.URI] =
    Coder.xmap(Coder.beam(StringUtf8Coder.of()))(s => new java.net.URI(s), _.toString)

  implicit def pathCoder: Coder[java.nio.file.Path] =
    Coder.xmap(Coder.beam(StringUtf8Coder.of()))(s => java.nio.file.Paths.get(s), _.toString)

  import java.lang.{Iterable => jIterable}
  implicit def jIterableCoder[T](implicit c: Coder[T]): Coder[jIterable[T]] =
    Coder.transform(c) { bc =>
      Coder.beam(bcoders.IterableCoder.of(bc))
    }

  implicit def jlistCoder[T](implicit c: Coder[T]): Coder[java.util.List[T]] =
    Coder.transform(c) { bc =>
      Coder.beam(bcoders.ListCoder.of(bc))
    }

  implicit def jMapCoder[K, V](implicit ck: Coder[K], cv: Coder[V]): Coder[java.util.Map[K, V]] =
    Coder.transform(ck) { bk =>
      Coder.transform(cv) { bv =>
        Coder.beam(bcoders.MapCoder.of(bk, bv))
      }
    }

  private def fromScalaCoder[J <: java.lang.Number, S <: AnyVal](coder: Coder[S]): Coder[J] =
    coder.asInstanceOf[Coder[J]]

  implicit val jShortCoder: Coder[java.lang.Short] = fromScalaCoder(Coder.shortCoder)
  implicit val jByteCoder: Coder[java.lang.Byte] = fromScalaCoder(Coder.byteCoder)
  implicit val jIntegerCoder: Coder[java.lang.Integer] = fromScalaCoder(Coder.intCoder)
  implicit val jLongCoder: Coder[java.lang.Long] = fromScalaCoder(Coder.longCoder)
  implicit val jFloatCoder: Coder[java.lang.Float] = fromScalaCoder(Coder.floatCoder)
  implicit val jDoubleCoder: Coder[java.lang.Double] = fromScalaCoder(Coder.doubleCoder)

  import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow}
  implicit def intervalWindowCoder: Coder[IntervalWindow] =
    Coder.beam(IntervalWindow.getCoder)

  implicit def boundedWindowCoder: Coder[BoundedWindow] =
    Coder.kryo[BoundedWindow]

  implicit def serializableCoder: Coder[Serializable] = Coder.kryo[Serializable]

  import org.apache.beam.sdk.transforms.windowing.PaneInfo
  implicit def paneInfoCoder: Coder[PaneInfo] =
    Coder.beam(PaneInfo.PaneInfoCoder.of())

  implicit def tablerowCoder: Coder[com.google.api.services.bigquery.model.TableRow] =
    Coder.beam(org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder.of())
  implicit def messageCoder: Coder[org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage] =
    Coder.beam(org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder.of())

  import java.nio.ByteBuffer
  implicit def byteBufferCoder: Coder[ByteBuffer] =
    Coder.beam(new ByteBufferCoder())

  implicit def beamKVCoder[K: Coder, V: Coder]: Coder[KV[K, V]] =
    Coder.kv(Coder[K], Coder[V])
}
