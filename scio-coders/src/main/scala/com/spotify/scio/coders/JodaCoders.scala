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

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import org.apache.beam.sdk.coders.AtomicCoder
import org.joda.time.chrono.ISOChronology
import org.joda.time.{Chronology, LocalDate, LocalDateTime, LocalTime}

trait JodaCoders {
  implicit def jodaLocalDateTimeCoder: Coder[LocalDateTime] = Coder.beam(new JodaLocalDateTimeCoder)
  implicit def jodaLocalDateCoder: Coder[LocalDate] = Coder.beam(new JodaLocalDateCoder)
  implicit def jodaLocalTimeCoder: Coder[LocalTime] = Coder.beam(new JodaLocalTimeCoder)
}

object JodaCoders {
  def checkChronology(chronology: Chronology): Unit = {
    if (chronology != null && chronology != ISOChronology.getInstanceUTC) {
      throw new IllegalArgumentException(s"Unsupported chronology: $chronology")
    }
  }
}

private final class JodaLocalDateTimeCoder extends AtomicCoder[LocalDateTime] {
  import JodaCoders.checkChronology

  def encode(value: LocalDateTime, os: OutputStream): Unit = {
    checkChronology(value.getChronology)
    val dos = new DataOutputStream(os)

    dos.writeShort(value.getYear)
    dos.writeShort(value.getMonthOfYear)
    dos.writeShort(value.getDayOfMonth)
    dos.writeShort(value.getHourOfDay)
    dos.writeShort(value.getMinuteOfHour)
    dos.writeShort(value.getSecondOfMinute)
    dos.writeShort(value.getMillisOfSecond)
  }

  def decode(is: InputStream): LocalDateTime = {
    val dis = new DataInputStream(is)

    val year = dis.readShort()
    val month = dis.readShort()
    val day = dis.readShort()
    val hour = dis.readShort()
    val minute = dis.readShort()
    val second = dis.readShort()
    val ms = dis.readShort()

    new LocalDateTime(year, month, day, hour, minute, second, ms)
  }
}

private final class JodaLocalDateCoder extends AtomicCoder[LocalDate] {
  import JodaCoders.checkChronology

  def encode(value: LocalDate, os: OutputStream): Unit = {
    checkChronology(value.getChronology)
    val dos = new DataOutputStream(os)

    dos.writeShort(value.getYear)
    dos.writeShort(value.getMonthOfYear)
    dos.writeShort(value.getDayOfMonth)
  }

  def decode(is: InputStream): LocalDate = {
    val dis = new DataInputStream(is)

    val year = dis.readShort()
    val month = dis.readShort()
    val day = dis.readShort()

    new LocalDate(year, month, day)
  }
}

private final class JodaLocalTimeCoder extends AtomicCoder[LocalTime] {
  import JodaCoders.checkChronology

  def encode(value: LocalTime, os: OutputStream): Unit = {
    checkChronology(value.getChronology)
    val dos = new DataOutputStream(os)

    dos.writeShort(value.getHourOfDay)
    dos.writeShort(value.getMinuteOfHour)
    dos.writeShort(value.getSecondOfMinute)
    dos.writeShort(value.getMillisOfSecond)
  }

  def decode(is: InputStream): LocalTime = {
    val dis = new DataInputStream(is)

    val hour = dis.readShort()
    val minute = dis.readShort()
    val second = dis.readShort()
    val ms = dis.readShort()

    new LocalTime(hour, minute, second, ms)
  }
}
