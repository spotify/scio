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

package com.spotify.scio.extra.bigquery

import com.spotify.scio.extra.bigquery.AvroConverters.AvroConversionException

import java.math.{BigDecimal => JBigDecimal}
import java.nio.ByteBuffer
import java.util

import com.spotify.scio.bigquery.TableRow
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericFixed, IndexedRecord}
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalTime, ZoneOffset}
import scala.jdk.CollectionConverters._

private object ToTableRow {
  private lazy val EncodingPropName: String = "bigquery.bytes.encoder"
  private lazy val Base64Encoding: BaseEncoding = BaseEncoding.base64()
  private lazy val HexEncoding: BaseEncoding = BaseEncoding.base16()

  // YYYY-[M]M-[D]D
  private lazy val JodaLocalDateFormatter =
    org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
  private lazy val LocalDateFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)

  // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
  private lazy val JodaLocalTimeFormatter =
    org.joda.time.format.DateTimeFormat.forPattern("HH:mm:ss.SSSSSS")
  private lazy val LocalTimeFormatter =
    DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")

  // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
  private lazy val JodaTimestampFormatter =
    org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
  private lazy val TimestampFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)
}

/**
 * Converts an [[org.apache.avro.generic.IndexedRecord IndexedRecord]] into a
 * [[com.spotify.scio.bigquery.TableRow TableRow]].
 */
private[bigquery] trait ToTableRow {
  import ToTableRow._

  private[bigquery] def toTableRowField(fieldValue: Any, field: Schema.Field): Any =
    fieldValue match {
      case x: CharSequence            => x.toString
      case x: EnumSymbol              => x.toString
      case x: Enum[_]                 => x.name()
      case x: JBigDecimal             => x.toString
      case x: Number                  => x
      case x: Boolean                 => x
      case x: GenericFixed            => encodeByteArray(x.bytes(), field.schema())
      case x: ByteBuffer              => encodeByteArray(toByteArray(x), field.schema())
      case x: util.Map[_, _]          => toTableRowFromMap(x.asScala, field)
      case x: java.lang.Iterable[_]   => toTableRowFromIterable(x.asScala, field)
      case x: IndexedRecord           => AvroConverters.toTableRow(x)
      case x: LocalDate               => LocalDateFormatter.format(x)
      case x: LocalTime               => LocalTimeFormatter.format(x)
      case x: Instant                 => TimestampFormatter.format(x)
      case x: org.joda.time.LocalDate => JodaLocalDateFormatter.print(x)
      case x: org.joda.time.LocalTime => JodaLocalTimeFormatter.print(x)
      case x: org.joda.time.DateTime  => JodaTimestampFormatter.print(x)
      case _                          =>
        throw AvroConversionException(
          s"ToTableRow conversion failed:" +
            s"could not match ${fieldValue.getClass}"
        )
    }

  private def toTableRowFromIterable(iterable: Iterable[Any], field: Schema.Field): util.List[_] =
    iterable
      .map { item =>
        if (item.isInstanceOf[Iterable[_]] || item.isInstanceOf[Map[_, _]]) {
          throw AvroConversionException(
            s"ToTableRow conversion failed for item $item: " +
              s"iterable and map types not supported"
          )
        }
        toTableRowField(item, field)
      }
      .toList
      .asJava

  private def toTableRowFromMap(map: Iterable[(Any, Any)], field: Schema.Field): util.List[_] =
    map
      .map { case (k, v) =>
        new TableRow()
          .set("key", toTableRowField(k, field))
          .set("value", toTableRowField(v, field))
      }
      .toList
      .asJava

  private def encodeByteArray(bytes: Array[Byte], fieldSchema: Schema): String =
    Option(fieldSchema.getProp(EncodingPropName)) match {
      case Some("BASE64") => Base64Encoding.encode(bytes)
      case Some("HEX")    => HexEncoding.encode(bytes)
      case Some(encoding) =>
        throw AvroConversionException(s"Unsupported encoding $encoding")
      case None => Base64Encoding.encode(bytes)
    }

  private def toByteArray(buffer: ByteBuffer) = {
    val copy = buffer.asReadOnlyBuffer
    val bytes = new Array[Byte](copy.limit)
    copy.rewind
    copy.get(bytes)
    bytes
  }
}
