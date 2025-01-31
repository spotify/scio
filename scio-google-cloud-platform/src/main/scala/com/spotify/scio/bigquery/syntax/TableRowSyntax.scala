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

package com.spotify.scio.bigquery.syntax

import com.google.common.io.BaseEncoding
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types._
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

/**
 * Enhanced version of [[TableRow]] with typed getters.
 *
 * Maximize compatibility by allowing
 *   - boxed java type
 *   - string values
 */
object TableRowOps {

  def boolean(value: AnyRef): Boolean = value match {
    case x: java.lang.Boolean => Boolean.unbox(x)
    case x: String            => x.toBoolean
    case _ => throw new UnsupportedOperationException("Cannot convert to boolean: " + value)
  }

  def int(value: AnyRef): Int = value match {
    case x: java.lang.Number => x.intValue()
    case x: String           => x.toInt
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to integer: " + value)
  }

  def long(value: AnyRef): Long = value match {
    case x: java.lang.Number => x.longValue()
    case x: String           => x.toLong
    case _ => throw new UnsupportedOperationException("Cannot convert to long: " + value)
  }

  def float(value: AnyRef): Float = value match {
    case x: java.lang.Number => x.floatValue()
    case x: String           => x.toFloat
    case _ => throw new UnsupportedOperationException("Cannot convert to float: " + value)
  }

  def double(value: AnyRef): Double = value match {
    case x: java.lang.Number => x.doubleValue()
    case x: String           => x.toDouble
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to double: " + value)
  }

  def string(value: AnyRef): String = value match {
    case x: java.lang.String => x
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to string: " + value)
  }

  def numeric(value: AnyRef): BigDecimal = value match {
    case x: scala.math.BigDecimal => Numeric(x)
    case x: java.math.BigDecimal  => Numeric(BigDecimal(x))
    case x: java.lang.Number      => Numeric(x.toString)
    case x: String                => Numeric(x)
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to numeric: " + value)
  }

  def bytes(value: AnyRef): Array[Byte] = value match {
    case x: Array[Byte] => x
    case x: String      => BaseEncoding.base64().decode(x)
    case _ => throw new UnsupportedOperationException("Cannot convert to bytes: " + value)
  }

  def timestamp(value: AnyRef): Instant = value match {
    case x: Instant          => x
    case x: java.lang.String => Timestamp.parse(x)
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to timestamp: " + value)
  }

  def date(value: AnyRef): LocalDate = value match {
    case x: LocalDate => x
    case x: String    => Date.parse(x)
    case _            => throw new UnsupportedOperationException("Cannot convert to date: " + value)
  }

  def time(value: AnyRef): LocalTime = value match {
    case x: LocalTime => x
    case x: String    => Time.parse(x)
    case _            => throw new UnsupportedOperationException("Cannot convert to time: " + value)
  }

  def datetime(value: AnyRef): LocalDateTime = value match {
    case x: LocalDateTime => x
    case x: String        => DateTime.parse(x)
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to datetime: " + value)
  }

  def geography(value: AnyRef): Geography = value match {
    case x: Geography => x
    case x: String    => Geography(x)
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to geography: " + value)
  }

  def json(value: AnyRef): Json = value match {
    case x: Json   => x
    case x: String => Json(x)
    case _         => throw new UnsupportedOperationException("Cannot convert to json: " + value)
  }

  def bignumeric(value: AnyRef): BigNumeric = value match {
    case x: BigNumeric           => x
    case x: java.math.BigDecimal => BigNumeric(x)
    case x: java.lang.Number     => BigNumeric(x.toString)
    case x: String               => BigNumeric(x)
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to bigNumeric: " + value)
  }

  def record(value: AnyRef): TableRow = value match {
    case x: java.util.Map[String @unchecked, AnyRef @unchecked] =>
      x.asScala.foldLeft(new TableRow()) { case (tr, (name, value)) =>
        tr.set(name, value)
      }
    case _ =>
      throw new UnsupportedOperationException("Cannot convert to record: " + value)
  }

  def cast[T](name: AnyRef, fn: AnyRef => T)(value: AnyRef): T = try {
    fn(value)
  } catch {
    case e: UnsupportedOperationException =>
      throw new UnsupportedOperationException("Field cannot be converted: " + name, e)
  }

  def required(name: AnyRef)(r: TableRow): AnyRef = {
    val x = r.get(name)
    if (x == null) throw new NoSuchElementException("Field not found: " + name)
    x
  }

  def nullable(name: AnyRef)(r: TableRow): Option[AnyRef] = Option(r.get(name))

  def repeated(name: AnyRef)(r: TableRow): List[AnyRef] = required(name)(r) match {
    case l: java.util.List[AnyRef @unchecked] => l.iterator().asScala.toList
    case _ => throw new UnsupportedOperationException("Field is not repeated: " + name)
  }
}

final class TableRowOps(private val r: TableRow) extends AnyVal {
  import TableRowOps._

  def getRequired(name: AnyRef): AnyRef = required(name)(r)
  def getNullable(name: AnyRef): Option[AnyRef] = nullable(name)(r)
  def getRepeated(name: AnyRef): List[AnyRef] = repeated(name)(r)

  /////////////////////////////////////////////////////////////////////////////
  def getBoolean(name: AnyRef): Boolean = getRequired(name).pipe(cast(name, boolean))
  def getBooleanOpt(name: AnyRef): Option[Boolean] = getNullable(name).map(cast(name, boolean))
  def getBooleanList(name: AnyRef): List[Boolean] = getRepeated(name).map(cast(name, boolean))

  /////////////////////////////////////////////////////////////////////////////
  def getInt(name: AnyRef): Int = getRequired(name).pipe(cast(name, int))
  def getIntOpt(name: AnyRef): Option[Int] = getNullable(name).map(cast(name, int))
  def getIntList(name: AnyRef): List[Int] = getRepeated(name).map(cast(name, int))

  /////////////////////////////////////////////////////////////////////////////
  def getLong(name: AnyRef): Long = getRequired(name).pipe(cast(name, long))
  def getLongOpt(name: AnyRef): Option[Long] = getNullable(name).map(cast(name, long))
  def getLongList(name: AnyRef): List[Long] = getRepeated(name).map(cast(name, long))

  /////////////////////////////////////////////////////////////////////////////
  def getFloat(name: AnyRef): Float = getRequired(name).pipe(cast(name, float))
  def getFloatOpt(name: AnyRef): Option[Float] = getNullable(name).map(cast(name, float))
  def getFloatList(name: AnyRef): List[Float] = getRepeated(name).map(cast(name, float))

  /////////////////////////////////////////////////////////////////////////////
  def getDouble(name: AnyRef): Double = getRequired(name).pipe(cast(name, double))
  def getDoubleOpt(name: AnyRef): Option[Double] = getNullable(name).map(cast(name, double))
  def getDoubleList(name: AnyRef): List[Double] = getRepeated(name).map(cast(name, double))

  /////////////////////////////////////////////////////////////////////////////
  def getString(name: AnyRef): String = getRequired(name).pipe(cast(name, string))
  def getStringOpt(name: AnyRef): Option[String] = getNullable(name).map(cast(name, string))
  def getStringList(name: AnyRef): List[String] = getRepeated(name).map(cast(name, string))

  /////////////////////////////////////////////////////////////////////////////
  def getNumeric(name: AnyRef): BigDecimal = getRequired(name).pipe(cast(name, numeric))
  def getNumericOpt(name: AnyRef): Option[BigDecimal] = getNullable(name).map(cast(name, numeric))
  def getNumericList(name: AnyRef): List[BigDecimal] = getRepeated(name).map(cast(name, numeric))

  /////////////////////////////////////////////////////////////////////////////
  def getBytes(name: AnyRef): Array[Byte] = getRequired(name).pipe(cast(name, bytes))
  def getBytesOpt(name: AnyRef): Option[Array[Byte]] = getNullable(name).map(cast(name, bytes))
  def getBytesList(name: AnyRef): List[Array[Byte]] = getRepeated(name).map(cast(name, bytes))

  /////////////////////////////////////////////////////////////////////////////
  def getTimestamp(name: AnyRef): Instant = getRequired(name).pipe(cast(name, timestamp))
  def getTimestampOpt(name: AnyRef): Option[Instant] = getNullable(name).map(cast(name, timestamp))
  def getTimestampList(name: AnyRef): List[Instant] = getRepeated(name).map(cast(name, timestamp))

  /////////////////////////////////////////////////////////////////////////////
  def getDate(name: AnyRef): LocalDate = getRequired(name).pipe(cast(name, date))
  def getDateOpt(name: AnyRef): Option[LocalDate] = getNullable(name).map(cast(name, date))
  def getDateList(name: AnyRef): List[LocalDate] = getRepeated(name).map(cast(name, date))

  /////////////////////////////////////////////////////////////////////////////
  def getTime(name: AnyRef): LocalTime = getRequired(name).pipe(cast(name, time))
  def getTimeOpt(name: AnyRef): Option[LocalTime] = getNullable(name).map(cast(name, time))
  def getTimeList(name: AnyRef): List[LocalTime] = getRepeated(name).map(cast(name, time))

  /////////////////////////////////////////////////////////////////////////////
  def getDateTime(name: AnyRef): LocalDateTime = getRequired(name).pipe(cast(name, datetime))
  def getDateTimeOpt(name: AnyRef): Option[LocalDateTime] =
    getNullable(name).map(cast(name, datetime))
  def getDateTimeList(name: AnyRef): List[LocalDateTime] =
    getRepeated(name).map(cast(name, datetime))

  /////////////////////////////////////////////////////////////////////////////
  def getGeography(name: AnyRef): Geography =
    getRequired(name).pipe(cast(name, geography))
  def getGeographyOpt(name: AnyRef): Option[Geography] =
    getNullable(name).map(cast(name, geography))
  def getGeographyList(name: AnyRef): List[Geography] =
    getRepeated(name).map(cast(name, geography))

  /////////////////////////////////////////////////////////////////////////////
  def getJson(name: AnyRef): Json = getRequired(name).pipe(cast(name, json))
  def getJsonOpt(name: AnyRef): Option[Json] = getNullable(name).map(cast(name, json))
  def getJsonList(name: AnyRef): List[Json] = getRepeated(name).map(cast(name, json))

  /////////////////////////////////////////////////////////////////////////////
  def getBigNumeric(name: AnyRef): BigNumeric =
    getRequired(name).pipe(cast(name, bignumeric))
  def getBigNumericOpt(name: AnyRef): Option[BigNumeric] =
    getNullable(name).map(cast(name, bignumeric))
  def getBigNumericList(name: AnyRef): List[BigNumeric] =
    getRepeated(name).map(cast(name, bignumeric))

  /////////////////////////////////////////////////////////////////////////////
  def getRecord(name: AnyRef): TableRow = getRequired(name).pipe(cast(name, record))
  def getRecordOpt(name: AnyRef): Option[TableRow] = getNullable(name).map(cast(name, record))
  def getRecordList(name: AnyRef): List[TableRow] = getRepeated(name).map(cast(name, record))
}

trait TableRowSyntax {
  implicit def bigQueryTableRowOps(tr: TableRow): TableRowOps = new TableRowOps(tr)
}
