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

package com.spotify.scio.bigquery

import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.scalatest.{FlatSpec, Matchers}

class TimestampTest extends FlatSpec with Matchers {

  "Timestamp" should "round trip Instant" in {
    val now = Instant.now()
    Timestamp.parse(Timestamp(now)) should equal (now)
  }

  it should "round trip Long" in {
    val now = Instant.now()
    Timestamp.parse(Timestamp(now.getMillis)) should equal (now)
  }

  it should "parse different formats" in {
    val t = Timestamp.parse("2016-01-01 00:00:00.000000 UTC")
    Timestamp.parse("2016-01-01 00:00:00.000000 UTC") should equal (t)
    Timestamp.parse("2016-01-01 08:00:00.000000+08:00") should equal (t)
    Timestamp.parse("2016-01-01 08:00:00.000000+0800") should equal (t)
    Timestamp.parse("2016-01-01 08:00:00.000000+08") should equal (t)
    Timestamp.parse("2016-01-01 00:00:00.000 UTC") should equal (t)
    Timestamp.parse("2016-01-01 00:00:00.000") should equal (t)
    Timestamp.parse("2016-01-01 00:00:00 UTC") should equal (t)
    Timestamp.parse("2016-01-01 00:00:00") should equal (t)
    Timestamp.parse("2016-01-01 UTC") should equal (t)
    Timestamp.parse("2016-01-01") should equal (t)
  }

  "Date" should "round trip LocalDate" in {
    val now = LocalDate.now()
    Date.parse(Date(now)) should equal (now)
  }

  "Time" should "round trip LocalTime" in {
    val now = LocalTime.now()
    Time.parse(Time(now)) should equal (now)
  }

  it should "parse different formats" in {
    val t = Time.parse("00:00:00.000000")
    Time.parse("00:00:00.000000") should equal (t)
    Time.parse("00:00:00.000") should equal (t)
    Time.parse("00:00:00") should equal (t)
  }

  "DateTime" should "round trip LocalDateTime" in {
    val now = LocalDateTime.now()
    DateTime.parse(DateTime(now)) should equal (now)
  }

  it should "parse different formats" in {
    val t = DateTime.parse("2016-01-01 00:00:00.000000")
    DateTime.parse("2016-01-01 00:00:00.000000") should equal (t)
    DateTime.parse("2016-01-01 00:00:00.000") should equal (t)
    DateTime.parse("2016-01-01 00:00:00") should equal (t)
    DateTime.parse("2016-01-01") should equal (t)
  }

}
