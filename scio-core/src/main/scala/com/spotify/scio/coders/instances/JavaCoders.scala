/*
 * Copyright 2019 Spotify AB.
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
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, Period}
import java.util.UUID
import com.spotify.scio.IsJavaBean
import com.spotify.scio.coders.{Coder, CoderGrammar}
import com.spotify.scio.schemas.Schema
import com.spotify.scio.transforms.BaseAsyncLookupDoFn
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.coders.{Coder => _, _}
import org.apache.beam.sdk.schemas.SchemaCoder
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.{coders => bcoders}

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

private[coders] object VoidCoder extends AtomicCoder[Void] {
  override def encode(value: Void, outStream: OutputStream): Unit = ()

  override def decode(inStream: InputStream): Void = ???

  override def structuralValue(value: Void): AnyRef = AnyRef
}

//
// Java Coders
//
trait JavaCoders extends CoderGrammar with JavaBeanCoders {
  implicit lazy val voidCoder: Coder[Void] = beam[Void](VoidCoder)

  implicit lazy val uuidCoder: Coder[UUID] =
    xmap(Coder[(Long, Long)])(
      { case (msb, lsb) => new UUID(msb, lsb) },
      uuid => (uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
    )

  implicit lazy val uriCoder: Coder[java.net.URI] =
    xmap(beam(StringUtf8Coder.of()))(s => new java.net.URI(s), _.toString)

  implicit lazy val pathCoder: Coder[java.nio.file.Path] =
    xmap(beam(StringUtf8Coder.of()))(s => java.nio.file.Paths.get(s), _.toString)

  implicit def jIterableCoder[T](implicit c: Coder[T]): Coder[java.lang.Iterable[T]] =
    transform(c)(bc => beam(bcoders.IterableCoder.of(bc)))

  implicit def jListCoder[T](implicit c: Coder[T]): Coder[java.util.List[T]] =
    transform(c)(bc => beam(bcoders.ListCoder.of(bc)))

  implicit def jArrayListCoder[T](implicit c: Coder[T]): Coder[java.util.ArrayList[T]] =
    xmap(jListCoder[T])(new java.util.ArrayList(_), identity)

  implicit def jMapCoder[K, V](implicit ck: Coder[K], cv: Coder[V]): Coder[java.util.Map[K, V]] =
    transform(ck)(bk => transform(cv)(bv => beam(bcoders.MapCoder.of(bk, bv))))

  implicit def jTryCoder[A](implicit c: Coder[A]): Coder[BaseAsyncLookupDoFn.Try[A]] =
    xmap(ScalaCoders.tryCoder[A])(
      {
        case Success(value)     => new BaseAsyncLookupDoFn.Try(value)
        case Failure(exception) => new BaseAsyncLookupDoFn.Try[A](exception)
      },
      t => if (t.isSuccess) Success(t.get()) else Failure(t.getException)
    )

  implicit lazy val jBitSetCoder: Coder[java.util.BitSet] = beam(BitSetCoder.of())

  private def fromScalaCoder[J <: java.lang.Number, S <: AnyVal](coder: Coder[S]): Coder[J] =
    coder.asInstanceOf[Coder[J]]

  implicit lazy val jShortCoder: Coder[java.lang.Short] = fromScalaCoder(ScalaCoders.shortCoder)
  implicit lazy val jByteCoder: Coder[java.lang.Byte] = fromScalaCoder(ScalaCoders.byteCoder)
  implicit lazy val jIntegerCoder: Coder[java.lang.Integer] = fromScalaCoder(ScalaCoders.intCoder)
  implicit lazy val jLongCoder: Coder[java.lang.Long] = fromScalaCoder(ScalaCoders.longCoder)
  implicit lazy val jFloatCoder: Coder[java.lang.Float] = fromScalaCoder(ScalaCoders.floatCoder)
  implicit lazy val jDoubleCoder: Coder[java.lang.Double] = fromScalaCoder(ScalaCoders.doubleCoder)
  implicit lazy val jBooleanCoder: Coder[java.lang.Boolean] = beam(BooleanCoder.of())
  implicit lazy val jBigIntegerCoder: Coder[BigInteger] = beam(BigIntegerCoder.of())
  implicit lazy val jBigDecimalCoder: Coder[BigDecimal] = beam(BigDecimalCoder.of())
  implicit lazy val serializableCoder: Coder[Serializable] = kryo[Serializable]

  implicit lazy val jInstantCoder: Coder[Instant] =
    xmap(Coder[(Long, Int)])(
      { case (epochSeconds, nanoAdjustment) =>
        Instant.ofEpochSecond(epochSeconds, nanoAdjustment.toLong)
      },
      instant => (instant.getEpochSecond, instant.getNano)
    )

  implicit lazy val jLocalDateCoder: Coder[LocalDate] =
    xmap(Coder[(Int, Int, Int)])(
      { case (year, month, day) => LocalDate.of(year, month, day) },
      localDate => (localDate.getYear, localDate.getMonthValue, localDate.getDayOfMonth)
    )

  implicit lazy val jLocalTimeCoder: Coder[LocalTime] =
    xmap(Coder[(Int, Int, Int, Int)])(
      { case (hour, minute, second, nanoOfSecond) =>
        LocalTime.of(hour, minute, second, nanoOfSecond)
      },
      localTime => (localTime.getHour, localTime.getMinute, localTime.getSecond, localTime.getNano)
    )

  implicit lazy val jLocalDateTimeCoder: Coder[LocalDateTime] =
    xmap(Coder[(LocalDate, LocalTime)])(
      { case (localDate, localTime) => LocalDateTime.of(localDate, localTime) },
      localDateTime => (localDateTime.toLocalDate, localDateTime.toLocalTime)
    )

  implicit lazy val jDurationCoder: Coder[Duration] =
    xmap(Coder[(Long, Int)])(
      { case (seconds, nanoAdjustment) => Duration.ofSeconds(seconds, nanoAdjustment.toLong) },
      duration => (duration.getSeconds, duration.getNano)
    )

  implicit lazy val jPeriodCoder: Coder[Period] =
    xmap(Coder[(Int, Int, Int)])(
      { case (years, months, days) => Period.of(years, months, days) },
      period => (period.getYears, period.getMonths, period.getDays)
    )

  implicit lazy val jSqlTimestamp: Coder[java.sql.Timestamp] =
    xmap(jInstantCoder)(java.sql.Timestamp.from, _.toInstant())

  implicit lazy val jSqlDate: Coder[java.sql.Date] =
    xmap(jLocalDateCoder)(java.sql.Date.valueOf, _.toLocalDate())

  implicit lazy val jSqlTime: Coder[java.sql.Time] =
    xmap(jLocalTimeCoder)(java.sql.Time.valueOf, _.toLocalTime())

  implicit def coderJEnum[E <: java.lang.Enum[E]: ClassTag]: Coder[E] =
    xmap(Coder[String])(
      value => java.lang.Enum.valueOf(ScioUtil.classOf[E], value),
      _.name
    )
}

trait JavaBeanCoders extends CoderGrammar {
  def javaBeanCoder[T: IsJavaBean: ClassTag]: Coder[T] = {
    val rec = Schema.javaBeanSchema[T]
    val td = TypeDescriptor.of(ScioUtil.classOf[T])
    beam(SchemaCoder.of(rec.schema, td, rec.toRow, rec.fromRow))
  }
}

private[coders] object JavaCoders extends JavaCoders
