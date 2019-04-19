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

import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.bigquery._
import org.apache.beam.sdk.testing.PAssert
import org.joda.time.{DateTimeZone, Duration, Instant}
import org.scalatest._

import scala.collection.JavaConverters._

// Run BigQueryITUtil to re-populate tables for integration tests
class StorageIT extends FlatSpec with Matchers {
  import StorageIT._

  "fromStorage" should "work with REQUIRED fields" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      Required(
        true,
        i,
        i,
        BigDecimal(i),
        s"s$i",
        ByteString.copyFromUtf8(s"s$i"),
        t.plus(Duration.millis(i)),
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
    sc.close()
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
    sc.close()
  }

  it should "work with REPEATED fields" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      Repeated(
        List(true),
        List(i),
        List(i),
        List(BigDecimal(i)),
        List(s"s$i"),
        List(ByteString.copyFromUtf8(s"s$i")),
        List(t.plus(Duration.millis(i))),
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
    sc.close()
  }

  it should "work with selectedFields" in {
    val expected = (0 until 10).map { i =>
      (i.toLong, s"s$i", i.toLong)
    }.asJava
    val (sc, _) = ContextAndArgs(
      Array("--project=data-integration-test", "--tempLocation=gs://data-integration-test-eu/temp")
    )
    val p = sc
      .typedBigQuery[NestedWithFields]()
      .map { r =>
        (r.required.int, r.required.string, r.optional.get.int)
      }
      .internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.close()
  }

  it should "work with rowRestriction" in {
    val expected = (0 until 5).map { i =>
      (i.toLong, s"s$i", i.toLong, s"s$i", i.toLong, s"s$i")
    }.asJava
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
    sc.close()
  }

  it should "work with all options" in {
    val expected = (0 until 5).map { i =>
      (i.toLong, s"s$i", i.toLong)
    }.asJava
    val sc = ScioContext()
    val p = sc
      .typedBigQuery[NestedWithAll](NestedWithAll.table.format("nested"))
      .map { r =>
        (r.required.int, r.required.string, r.optional.get.int)
      }
      .internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.close()
  }

  it should "be consistent with fromTable" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      FromTable(
        true,
        i,
        i,
        BigDecimal(i),
        s"s$i",
        ByteString.copyFromUtf8(s"s$i"),
        t.plus(Duration.millis(i)),
        dt.toLocalDate.plusDays(i),
        dt.toLocalTime.plusMillis(i),
        dt.toLocalDateTime.plusMillis(i)
      )
    }.asJava
    val (sc, _) = ContextAndArgs(Array("--tempLocation=gs://data-integration-test-eu/temp"))
    val p = sc.typedBigQuery[FromTable]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.close()
  }

  it should "be consistent with fromQuery" in {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    val expected = (0 until 10).map { i =>
      FromQuery(
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
    val p = sc.typedBigQuery[FromQuery]().internal
    PAssert.that(p).containsInAnyOrder(expected)
    sc.close()
  }
}

object StorageIT {
  @BigQueryType.fromStorage("data-integration-test:storage.required")
  class Required

  @BigQueryType.fromStorage("data-integration-test:storage.optional")
  class Optional

  @BigQueryType.fromStorage("data-integration-test:storage.repeated")
  class Repeated

  @BigQueryType.fromStorage("data-integration-test:storage.nested",
                            selectedFields = List("required", "optional.int"))
  class NestedWithFields

  @BigQueryType.fromStorage("data-integration-test:storage.nested",
                            rowRestriction = "required.int < 5")
  class NestedWithRestriction

  @BigQueryType.fromStorage("data-integration-test:storage.%s",
                            List("nested"),
                            selectedFields = List("required", "optional.int"),
                            rowRestriction = "required.int < 5")
  class NestedWithAll

  @BigQueryType.fromTable("data-integration-test:storage.required")
  class FromTable

  @BigQueryType.fromQuery("SELECT * FROM `data-integration-test.storage.required`")
  class FromQuery
}
