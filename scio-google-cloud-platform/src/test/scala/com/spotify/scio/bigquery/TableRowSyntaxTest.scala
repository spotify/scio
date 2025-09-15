/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.bigquery

import com.google.common.io.BaseEncoding
import com.spotify.scio.bigquery.types.{Geography, Json}
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.util.CoderUtils
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

class TableRowSyntaxTest extends AnyFlatSpec with Matchers {

  private val bCoder = CoderMaterializer.beamWithDefault(Coder[TableRow])
  def serialize(tableRow: TableRow): TableRow = CoderUtils.clone(bCoder, tableRow)

  "TableRowOps" should "handle boolean" in {
    val row = serialize(
      new TableRow()
        .set("value", true)
        .set("other", "false")
        .set("list", List(true, false).asJava)
        .set("invalid", -1)
    )

    row.getBoolean("value") shouldBe true
    row.getBoolean("other") shouldBe false

    row.getBooleanOpt("value") shouldBe Some(true)
    row.getBooleanOpt("missing") shouldBe None

    row.getBooleanList("list") shouldBe List(true, false)

    val e = the[UnsupportedOperationException] thrownBy (row.getBoolean("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to boolean: -1"
  }

  it should "handle integer" in {
    val row = serialize(
      new TableRow()
        .set("value", 0)
        .set("other", "1")
        .set("list", List(0, 1).asJava)
        .set("invalid", true)
    )

    row.getInt("value") shouldBe 0
    row.getInt("other") shouldBe 1

    row.getIntOpt("value") shouldBe Some(0)
    row.getIntOpt("missing") shouldBe None

    row.getIntList("list") shouldBe List(0, 1)

    val e = the[UnsupportedOperationException] thrownBy (row.getInt("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to integer: true"
  }

  it should "handle long" in {
    val row = serialize(
      new TableRow()
        .set("value", 0L)
        .set("other", "1")
        .set("list", List(0L, 1L).asJava)
        .set("invalid", true)
    )

    row.getLong("value") shouldBe 0L
    row.getLong("other") shouldBe 1L

    row.getLongOpt("value") shouldBe Some(0L)
    row.getLongOpt("missing") shouldBe None

    row.getLongList("list") shouldBe List(0L, 1L)

    val e = the[UnsupportedOperationException] thrownBy (row.getLong("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to long: true"
  }

  it should "handle float" in {
    val row = serialize(
      new TableRow()
        .set("value", 0.0f)
        .set("other", "1.1")
        .set("list", List(0.0f, 1.1f).asJava)
        .set("invalid", true)
    )

    row.getFloat("value") shouldBe 0.0f
    row.getFloat("other") shouldBe 1.1f

    row.getFloatOpt("value") shouldBe Some(0.0f)
    row.getFloatOpt("missing") shouldBe None

    row.getFloatList("list") shouldBe List(0.0f, 1.1f)

    val e = the[UnsupportedOperationException] thrownBy (row.getFloat("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to float: true"
  }

  it should "handle double" in {
    val row = serialize(
      new TableRow()
        .set("value", 0.0)
        .set("other", "1.1")
        .set("list", List(0.0, 1.1).asJava)
        .set("invalid", true)
    )

    row.getDouble("value") shouldBe 0.0
    row.getDouble("other") shouldBe 1.1

    row.getDoubleOpt("value") shouldBe Some(0.0)
    row.getDoubleOpt("missing") shouldBe None

    row.getDoubleList("list") shouldBe List(0.0, 1.1)

    val e = the[UnsupportedOperationException] thrownBy (row.getDouble("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to double: true"
  }

  it should "handle string" in {
    val row = serialize(
      new TableRow()
        .set("value", "x")
        .set("list", List("x", "").asJava)
        .set("invalid", true)
    )

    row.getString("value") shouldBe "x"

    row.getStringOpt("value") shouldBe Some("x")
    row.getStringOpt("missing") shouldBe None

    row.getStringList("list") shouldBe List("x", "")

    val e = the[UnsupportedOperationException] thrownBy (row.getString("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to string: true"
  }

  it should "handle numeric" in {
    val bd1 = BigDecimal("1.23")
    val bd2 = BigDecimal("45.6")
    val row = serialize(
      new TableRow()
        .set("value", bd1.bigDecimal)
        .set("other", bd2.toString())
        .set("list", List(bd1.bigDecimal, bd2.bigDecimal).asJava)
        .set("invalid", true)
    )

    row.getNumeric("value") shouldBe bd1
    row.getNumeric("other") shouldBe bd2

    row.getNumericOpt("value") shouldBe Some(bd1)
    row.getNumericOpt("missing") shouldBe None

    row.getNumericList("list") shouldBe List(bd1, bd2)

    val e = the[UnsupportedOperationException] thrownBy (row.getNumeric("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to numeric: true"
  }

  it should "handle bytes" in {
    val helloWorldBytes = "hello world!".getBytes(StandardCharsets.UTF_8)
    val helloBytes = "hello".getBytes(StandardCharsets.UTF_8)
    val worldBytes = "world".getBytes(StandardCharsets.UTF_8)
    val row = serialize(
      new TableRow()
        .set("value", BaseEncoding.base64().encode(helloWorldBytes))
        .set(
          "list",
          List(
            BaseEncoding.base64().encode(helloBytes),
            BaseEncoding.base64().encode(worldBytes)
          ).asJava
        )
        .set("invalid", true)
    )

    implicit val eqArray: Equality[Array[Byte]] =
      (a: Array[Byte], b: Any) =>
        (a, b) match {
          case (abs, bbs: Array[Byte]) => abs.sameElements(bbs)
          case _                       => false
        }
    implicit val eqOpt: Equality[Option[Array[Byte]]] =
      (a: Option[Array[Byte]], b: Any) =>
        (a, b) match {
          case (None, None)                        => true
          case (Some(abs), Some(bbs: Array[Byte])) => abs === bbs
          case _                                   => false
        }

    implicit val listEq: Equality[List[Array[Byte]]] =
      (a: List[Array[Byte]], b: Any) =>
        (a, b) match {
          case (a, b: List[Array[Byte]]) if a.size == b.size =>
            a.zip(b).forall { case (abs, bbs) => abs === bbs }
          case _ => false
        }

    row.getBytes("value") should ===(helloWorldBytes)

    row.getBytesOpt("value") should ===(Some(helloWorldBytes))
    row.getBytesOpt("missing") should ===(None)

    row.getBytesList("list") should ===(List(helloBytes, worldBytes))

    val e = the[UnsupportedOperationException] thrownBy (row.getBytes("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to bytes: true"
  }

  it should "handle timestamp" in {
    val now = Instant.now()
    val future = now.plus(1000)
    val row = serialize(
      new TableRow()
        .set("value", now)
        .set("list", List(now, future).asJava)
        .set("invalid", true)
    )

    row.getTimestamp("value") shouldBe now

    row.getTimestampOpt("value") shouldBe Some(now)
    row.getTimestampOpt("missing") shouldBe None

    row.getTimestampList("list") shouldBe List(now, future)

    val e = the[UnsupportedOperationException] thrownBy (row.getTimestamp("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to timestamp: true"
  }

  it should "handle date" in {
    val today = LocalDate.now()
    val tomorrow = today.plusDays(1)
    val row = serialize(
      new TableRow()
        .set("value", today)
        .set("list", List(today, tomorrow).asJava)
        .set("invalid", true)
    )

    row.getDate("value") shouldBe today

    row.getDateOpt("value") shouldBe Some(today)
    row.getDateOpt("missing") shouldBe None

    row.getDateList("list") shouldBe List(today, tomorrow)

    val e = the[UnsupportedOperationException] thrownBy (row.getDate("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to date: true"
  }

  it should "handle time" in {
    val now = LocalTime.now()
    val future = now.plusHours(1)
    val row = serialize(
      new TableRow()
        .set("value", now)
        .set("list", List(now, future).asJava)
        .set("invalid", true)
    )

    row.getTime("value") shouldBe now

    row.getTimeOpt("value") shouldBe Some(now)
    row.getTimeOpt("missing") shouldBe None

    row.getTimeList("list") shouldBe List(now, future)

    val e = the[UnsupportedOperationException] thrownBy (row.getTime("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to time: true"
  }

  it should "handle datetime" in {
    val now = LocalDateTime.now()
    val future = now.plusMinutes(1)
    val row = serialize(
      new TableRow()
        .set("value", now)
        .set("list", List(now, future).asJava)
        .set("invalid", true)
    )

    row.getDateTime("value") shouldBe now

    row.getDateTimeOpt("value") shouldBe Some(now)
    row.getDateTimeOpt("missing") shouldBe None

    row.getDateTimeList("list") shouldBe List(now, future)

    val e = the[UnsupportedOperationException] thrownBy (row.getDateTime("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to datetime: true"
  }

  it should "handle geography" in {
    val point = "POINT(1 1)"
    val line = "LINESTRING(1 1, 2 1)"
    val row = serialize(
      new TableRow()
        .set("value", point)
        .set("list", List(point, line).asJava)
        .set("invalid", true)
    )

    row.getGeography("value") shouldBe Geography(point)

    row.getGeographyOpt("value") shouldBe Some(Geography(point))
    row.getGeographyOpt("missing") shouldBe None

    row.getGeographyList("list") shouldBe List(Geography(point), Geography(line))

    val e = the[UnsupportedOperationException] thrownBy (row.getGeography("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to geography: true"
  }

  it should "handle json" in {
    val kv = """{"key": "value"}"""
    val empty = "{}"
    val row = serialize(
      new TableRow()
        .set("value", kv)
        .set("list", List(kv, empty).asJava)
        .set("invalid", true)
    )

    row.getJson("value") shouldBe Json(kv)

    row.getJsonOpt("value") shouldBe Some(Json(kv))
    row.getJsonOpt("missing") shouldBe None

    row.getJsonList("list") shouldBe List(Json(kv), Json(empty))

    val e = the[UnsupportedOperationException] thrownBy (row.getJson("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to json: true"
  }

  it should "handle record" in {
    val kv = new TableRow().set("key", "value")
    val empty = new TableRow()
    val row = serialize(
      new TableRow()
        .set("value", kv)
        .set("list", List(kv, empty).asJava)
        .set("invalid", true)
    )

    row.getRecord("value") shouldBe kv

    row.getRecordOpt("value") shouldBe Some(kv)
    row.getRecordOpt("missing") shouldBe None

    row.getRecordList("list") shouldBe List(kv, empty)

    val e = the[UnsupportedOperationException] thrownBy (row.getRecord("invalid"))
    e.getMessage shouldBe "Field cannot be converted: invalid"
    e.getCause.getMessage shouldBe "Cannot convert to record: true"
  }
}
