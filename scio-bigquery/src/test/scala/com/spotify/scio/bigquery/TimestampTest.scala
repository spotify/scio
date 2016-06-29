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

import org.joda.time.{Instant, DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.scalatest.{Matchers, FlatSpec}

class TimestampTest extends FlatSpec with Matchers {

  "Timestamp" should "round trip Instant" in {
    val now = Instant.now()
    Timestamp.parse(Timestamp(now)) shouldBe now
  }

  it should "round trip Long" in {
    val now = Instant.now()
    Timestamp.parse(Timestamp(now.getMillis)) shouldBe now
  }

  it should "correctly parse timestamps with no fractional seconds" in {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    val now = new DateTime(DateTimeZone.UTC)
    Timestamp.parse(formatter.print(now)) shouldBe now.withMillisOfSecond(0)
  }

}
