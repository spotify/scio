/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.coders

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.joda.time.{DateTime, LocalDate, LocalDateTime, LocalTime}
import org.joda.time.chrono.ISOChronology
import org.scalatest.{FlatSpec, Matchers}

class JodaCodersTest extends FlatSpec with Matchers {
  "JodaDateTimeCoder" should "work" in {
    val coder = new JodaDateTimeCoder()
    val value = new DateTime(2018, 3, 12, 7, 15, 5, 900, ISOChronology.getInstanceUTC)
    val bytesOut = new ByteArrayOutputStream()
    coder.encode(value, bytesOut)

    val bytesIn = new ByteArrayInputStream(bytesOut.toByteArray)
    val decoded = coder.decode(bytesIn)
    decoded shouldEqual value
  }

  "JodaLocalDateTimeCoder" should "work" in {
    val coder = new JodaLocalDateTimeCoder()
    val value = new LocalDateTime(2018, 3, 12, 7, 15, 5, 900)
    val bytesOut = new ByteArrayOutputStream()
    coder.encode(value, bytesOut)

    val bytesIn = new ByteArrayInputStream(bytesOut.toByteArray)
    val decoded = coder.decode(bytesIn)
    decoded shouldEqual value
  }

  "JodaLocalDateCoder" should "work" in {
    val coder = new JodaLocalDateCoder()
    val value = new LocalDate(2018, 5, 12)
    val bytesOut = new ByteArrayOutputStream()
    coder.encode(value, bytesOut)

    val bytesIn = new ByteArrayInputStream(bytesOut.toByteArray)
    val decoded = coder.decode(bytesIn)
    decoded shouldEqual value
  }

  "JodaLocalTimeCoder" should "work" in {
    val coder = new JodaLocalTimeCoder()
    val value = new LocalTime(7, 30, 45, 100)
    val bytesOut = new ByteArrayOutputStream()
    coder.encode(value, bytesOut)

    val bytesIn = new ByteArrayInputStream(bytesOut.toByteArray)
    val decoded = coder.decode(bytesIn)
    decoded shouldEqual value
  }
}
