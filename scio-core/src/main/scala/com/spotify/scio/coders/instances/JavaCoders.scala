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

package com.spotify.scio.coders.instances

import java.io.{InputStream, OutputStream}
import java.math.{BigDecimal, BigInteger}
import java.time.Instant

import com.google.api.services.bigquery.model.TableRow
import com.spotify.scio.IsJavaBean
import com.spotify.scio.schemas.Schema
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.{Coder => _, _}
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage, PubsubMessageWithAttributesCoder}
import org.apache.beam.sdk.schemas.{Schema => BSchema, SchemaCoder}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow, PaneInfo}
import org.apache.beam.sdk.values.{KV, Row}
import org.apache.beam.sdk.{coders => bcoders}
import scala.reflect.ClassTag

private object VoidCoder extends AtomicCoder[Void] {
  override def encode(value: Void, outStream: OutputStream): Unit = ()

  override def decode(inStream: InputStream): Void = ???
}

//
// Java Coders
//
trait JavaCoders {

  implicit def voidCoder: Coder[Void] = Coder.beam[Void](VoidCoder)

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

  implicit def jArrayListCoder[T](implicit c: Coder[T]): Coder[java.util.ArrayList[T]] =
    Coder.xmap(jlistCoder[T])(new java.util.ArrayList(_), identity)

  implicit def jMapCoder[K, V](implicit ck: Coder[K], cv: Coder[V]): Coder[java.util.Map[K, V]] =
    Coder.transform(ck) { bk =>
      Coder.transform(cv) { bv =>
        Coder.beam(bcoders.MapCoder.of(bk, bv))
      }
    }

  implicit def jBitSetCoder: Coder[java.util.BitSet] = Coder.beam(BitSetCoder.of())

  private def fromScalaCoder[J <: java.lang.Number, S <: AnyVal](coder: Coder[S]): Coder[J] =
    coder.asInstanceOf[Coder[J]]

  implicit val jShortCoder: Coder[java.lang.Short] = fromScalaCoder(Coder.shortCoder)
  implicit val jByteCoder: Coder[java.lang.Byte] = fromScalaCoder(Coder.byteCoder)
  implicit val jIntegerCoder: Coder[java.lang.Integer] = fromScalaCoder(Coder.intCoder)
  implicit val jLongCoder: Coder[java.lang.Long] = fromScalaCoder(Coder.longCoder)
  implicit val jFloatCoder: Coder[java.lang.Float] = fromScalaCoder(Coder.floatCoder)
  implicit val jDoubleCoder: Coder[java.lang.Double] = fromScalaCoder(Coder.doubleCoder)

  implicit val jBooleanCoder: Coder[java.lang.Boolean] = Coder.beam(BooleanCoder.of())

  implicit def jBigIntegerCoder: Coder[BigInteger] = Coder.beam(BigIntegerCoder.of())

  implicit def jBigDecimalCoder: Coder[BigDecimal] = Coder.beam(BigDecimalCoder.of())

  implicit def intervalWindowCoder: Coder[IntervalWindow] = Coder.beam(IntervalWindow.getCoder)

  implicit def boundedWindowCoder: Coder[BoundedWindow] = Coder.kryo[BoundedWindow]

  implicit def serializableCoder: Coder[Serializable] = Coder.kryo[Serializable]

  implicit def paneInfoCoder: Coder[PaneInfo] = Coder.beam(PaneInfo.PaneInfoCoder.of())

  implicit def tableRowCoder: Coder[TableRow] = Coder.beam(TableRowJsonCoder.of())

  def row(schema: BSchema): Coder[Row] = Coder.beam(RowCoder.of(schema))

  implicit def messageCoder: Coder[PubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())

  implicit def beamKVCoder[K: Coder, V: Coder]: Coder[KV[K, V]] = Coder.kv(Coder[K], Coder[V])

  implicit def jInstantCoder: Coder[Instant] =
    Coder.xmap(Coder.jLongCoder)(Instant.ofEpochMilli(_), _.toEpochMilli)

  implicit def coderJEnum[E <: java.lang.Enum[_]: scala.reflect.ClassTag]: Coder[E] =
    Coder.kryo[E]

  implicit def javaBeanCoder[T: IsJavaBean: ClassTag]: Coder[T] = {
    val rec = Schema.javaBeanSchema[T]
    Coder.beam(SchemaCoder.of(rec.schema, rec.toRow, rec.fromRow))
  }
}
