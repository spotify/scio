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
    val expected = (0 until 10).map(newRequired).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[Required]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with OPTIONAL fields" in {
    val expected = (0 until 10).map(newOptional).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[Optional]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with REPEATED fields" in {
    val expected = (0 until 10).map(newRepeated).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[Repeated]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with nested fields" in {
    val expected = (0 until 10).map(newNested).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[Nested]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with selectedFields" in {
    val expected = (0 until 10).map(newNestedWithFields).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[NestedWithFields]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with rowRestriction" in {
    val expected = (0 until 5).map(nestedWithRestriction).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc.typedBigQuery[NestedWithRestriction]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.run()
  }

  it should "work with rowRestriction override" in {
    val expected = (0 until 3).map(nestedWithRestriction).asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p =
      sc.typedBigQueryStorage[NestedWithRestriction](rowRestriction = "required.int < 3").internal
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
        Some(i.toString),
        Some(ByteString.copyFromUtf8(i.toString)),
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
        i.toString,
        ByteString.copyFromUtf8(i.toString),
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
        Some(i.toString),
        Some(ByteString.copyFromUtf8(i.toString)),
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

  @BigQueryType.fromStorage("data-integration-test:storage.nested")
  class Nested

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

  @BigQueryType.fromStorage("data-integration-test:partition_a.table_%s", List("$LATEST"))
  class StorageLatest

  // to compile locally, you must first run `gcloud config set project data-integration-test`
  @BigQueryType.fromStorage("partition_a.table_%s", List("$LATEST"))
  class StorageEnvProject

  @BigQueryType.fromTable("data-integration-test:storage.required")
  class FromTable

  @BigQueryType.fromQuery("SELECT * FROM `data-integration-test.storage.required`")
  class FromQuery

  @BigQueryType.toTable
  case class ToTableRequired(bool: Boolean)

  private def newRequired(i: Int): Required = {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    Required(
      true,
      i.toLong,
      i.toDouble,
      BigDecimal(i),
      i.toString,
      ByteString.copyFromUtf8(i.toString),
      t.plus(Duration.millis(i.toLong)),
      dt.toLocalDate.plusDays(i),
      dt.toLocalTime.plusMillis(i),
      dt.toLocalDateTime.plusMillis(i)
    )
  }

  private def newOptional(i: Int): Optional = {
    if (i == 0) {
      Optional(
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
    } else {
      val t = new Instant(0)
      val dt = t.toDateTime(DateTimeZone.UTC)
      Optional(
        Some(true),
        Some(i.toLong),
        Some(i.toDouble),
        Some(BigDecimal(i)),
        Some(i.toString),
        Some(ByteString.copyFromUtf8(i.toString)),
        Some(t.plus(Duration.millis(i.toLong))),
        Some(dt.toLocalDate.plusDays(i)),
        Some(dt.toLocalTime.plusMillis(i)),
        Some(dt.toLocalDateTime.plusMillis(i))
      )
    }
  }

  private def newRepeated(i: Int): Repeated = {
    if (i == 0) {
      Repeated(
        Nil,
        Nil,
        Nil,
        Nil,
        Nil,
        Nil,
        Nil,
        Nil,
        Nil,
        Nil
      )
    } else {
      val t = new Instant(0)
      val dt = t.toDateTime(DateTimeZone.UTC)
      Repeated(
        List(true),
        List(i.toLong),
        List(i.toDouble),
        List(BigDecimal(i)),
        List(i.toString),
        List(ByteString.copyFromUtf8(i.toString)),
        List(t.plus(Duration.millis(i.toLong))),
        List(dt.toLocalDate.plusDays(i)),
        List(dt.toLocalTime.plusMillis(i)),
        List(dt.toLocalDateTime.plusMillis(i))
      )
    }
  }

  private def newNested(i: Int): Nested = {
    val required = Required$1(i, i.toString)
    if (i == 0) {
      Nested(required, None, Nil)
    } else {
      val optional = Optional$1(i, i.toString)
      val repeated = Repeated$1(i, i.toString)
      Nested(required, Some(optional), List(repeated))
    }
  }

  private def newNestedWithFields(i: Int): NestedWithFields = {
    val required = Required$2(i, i.toString)
    if (i == 0) {
      NestedWithFields(required, None)
    } else {
      val optional = Optional$2(i)
      NestedWithFields(required, Some(optional))
    }
  }

  private def nestedWithRestriction(i: Int): NestedWithRestriction = {
    val required = Required$3(i, i.toString)
    if (i == 0) {
      NestedWithRestriction(required, None, Nil)
    } else {
      val optional = Optional$3(i, i.toString)
      val repeated = Repeated$2(i, i.toString)
      NestedWithRestriction(required, Some(optional), List(repeated))
    }
  }
}
