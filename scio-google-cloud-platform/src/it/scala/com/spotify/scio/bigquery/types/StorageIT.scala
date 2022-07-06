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

package com.spotify.scio.bigquery.types

import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions
import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryTaps._
import com.spotify.scio.bigquery._
import com.spotify.scio.io.Taps
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.testing.PAssert
import org.joda.time.{DateTimeZone, Duration, Instant}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// scio-test/it:runMain com.spotify.scio.PopulateTestData to re-populate data for integration tests
class StorageIT extends AnyFlatSpec with Matchers {
  import StorageIT._

  "fromStorage" should "work with REQUIRED fields" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      Required(
        true,
        i.toLong,
        i.toDouble,
        BigDecimal(i),
        s"s$i",
        ByteString.copyFromUtf8(s"s$i"),
        t.plus(Duration.millis(i.toLong)),
        dt.toLocalDate.plusDays(i),
        dt.toLocalTime.plusMillis(i),
        dt.toLocalDateTime.plusMillis(i)
      )
    }.asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[Required]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with OPTIONAL fields" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      Optional(
        Some(true),
        Some(i),
        Some(i),
        Some(BigDecimal(i)),
        Some(s"s$i"),
        Some(ByteString.copyFromUtf8(s"s$i")),
        Some(t.plus(Duration.millis(i))),
        Some(dt.toLocalDate.plusDays(i)),
        Some(dt.toLocalTime.plusMillis(i)),
        Some(dt.toLocalDateTime.plusMillis(i))
      )
    }.asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[Optional]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with REPEATED fields" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      Repeated(
        List(true),
        List(i.toLong),
        List(i.toDouble),
        List(BigDecimal(i)),
        List(s"s$i"),
        List(ByteString.copyFromUtf8(s"s$i")),
        List(t.plus(Duration.millis(i.toLong))),
        List(dt.toLocalDate.plusDays(i)),
        List(dt.toLocalTime.plusMillis(i)),
        List(dt.toLocalDateTime.plusMillis(i))
      )
    }.asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[Repeated]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with selectedFields" in {
    val expected = (0 until 10).map(i => (i.toLong, s"s$i", i.toLong)).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc
      .typedBigQuery[NestedWithFields]()
      .map(r => (r.required.int, r.required.string, r.optional.get.int))
      .internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with rowRestriction" in {
    val expected =
      (0 until 5).map(i => (i.toLong, s"s$i", i.toLong, s"s$i", i.toLong, s"s$i")).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc
      .typedBigQuery[NestedWithRestriction]()
      .map { r =>
        val (req, opt, rep) = (r.required, r.optional.get, r.repeated.head)
        (req.int, req.string, opt.int, opt.string, rep.int, rep.string)
      }
      .internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with rowRestriction override" in {
    val expected =
      (0 until 3).map(i => (i.toLong, s"s$i", i.toLong, s"s$i", i.toLong, s"s$i")).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc
      .typedBigQueryStorage[NestedWithRestriction](rowRestriction = "required.int < 3")
      .map { r =>
        val (req, opt, rep) = (r.required, r.optional.get, r.repeated.head)
        (req.int, req.string, opt.int, opt.string, rep.int, rep.string)
      }
      .internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with all options" in {
    val expected = (0 until 5).map(i => (i.toLong, s"s$i", i.toLong)).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc
      .typedBigQuery[NestedWithAll](Table.Spec(NestedWithAll.table.format("nested")))
      .map(r => (r.required.int, r.required.string, r.optional.get.int))
      .internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with query" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      FromQuery(
        Some(true),
        Some(i.toLong),
        Some(i.toDouble),
        Some(BigDecimal(i)),
        Some(s"s$i"),
        Some(ByteString.copyFromUtf8(s"s$i")),
        Some(t.plus(Duration.millis(i.toLong))),
        Some(dt.toLocalDate.plusDays(i)),
        Some(dt.toLocalTime.plusMillis(i)),
        Some(dt.toLocalDateTime.plusMillis(i))
      )
    }.asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )

    val p = sc.typedBigQueryStorage[FromQuery]().internal
    PAssert.that(p).containsInAnyOrder(expected)

    sc.run()
  }

  it should "be consistent with fromTable" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      FromTable(
        true,
        i.toLong,
        i.toDouble,
        BigDecimal(i),
        s"s$i",
        ByteString.copyFromUtf8(s"s$i"),
        t.plus(Duration.millis(i.toLong)),
        dt.toLocalDate.plusDays(i),
        dt.toLocalTime.plusMillis(i),
        dt.toLocalDateTime.plusMillis(i)
      )
    }.asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[FromTable]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with toTable" in {
    val expected = (0 until 10).map(_ => ToTableRequired(true)).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc
      .typedBigQueryStorage[ToTableRequired](Table.Spec("data-integration-test:storage.required"))
      .internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "be consistent with fromQuery" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      FromQuery(
        Some(true),
        Some(i.toLong),
        Some(i.toDouble),
        Some(BigDecimal(i)),
        Some(s"s$i"),
        Some(ByteString.copyFromUtf8(s"s$i")),
        Some(t.plus(Duration.millis(i.toLong))),
        Some(dt.toLocalDate.plusDays(i)),
        Some(dt.toLocalTime.plusMillis(i)),
        Some(dt.toLocalDateTime.plusMillis(i))
      )
    }.asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[FromQuery]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  "Tap" should "support read" in {
    val tableRef = beam.BigQueryHelpers.parseTableSpec("data-integration-test:storage.required")
    val futureTap = Taps().bigQueryStorage(tableRef, TableReadOptions.newBuilder().build())

    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    val res = Await.result(futureTap, Duration.Inf).value.toList

    res should not be empty
  }

  it should "support typed read" in {
    val tableRef = beam.BigQueryHelpers.parseTableSpec("data-integration-test:storage.required")
    val futureTap =
      Taps().typedBigQueryStorage[Required](tableRef, TableReadOptions.newBuilder().build())

    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    val res = Await.result(futureTap, Duration.Inf).value.toList

    res should not be empty
  }
}

object StorageIT {
  @BigQueryType.fromStorage("data-integration-test:storage.required")
  class Required

  @BigQueryType.fromStorage("data-integration-test:storage.optional")
  class Optional

  @BigQueryType.fromStorage("data-integration-test:storage.repeated")
  class Repeated

  @BigQueryType.fromStorage(
    "data-integration-test:storage.nested",
    selectedFields = List("required", "optional.int")
  )
  class NestedWithFields

  @BigQueryType.fromStorage(
    "data-integration-test:storage.nested",
    rowRestriction = "required.int < 5"
  )
  class NestedWithRestriction

  @BigQueryType.fromStorage(
    "data-integration-test:storage.%s",
    List("nested"),
    selectedFields = List("required", "optional.int"),
    rowRestriction = "required.int < 5"
  )
  class NestedWithAll

  @BigQueryType.fromStorage("data-integration-test:partition_a.table_%s", List("$LATEST"))
  class StorageLatest

  @BigQueryType.fromStorage("partition_a.table_%s", List("$LATEST"))
  class StorageEnvProject

  @BigQueryType.fromTable("data-integration-test:storage.required")
  class FromTable

  @BigQueryType.fromQuery("SELECT * FROM `data-integration-test.storage.required`")
  class FromQuery

  @BigQueryType.toTable
  case class ToTableRequired(bool: Boolean)
}
