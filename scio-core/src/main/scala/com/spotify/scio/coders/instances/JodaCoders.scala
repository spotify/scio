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

package com.spotify.scio.coders.instances

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.{AtomicCoder, InstantCoder}
import org.joda.time.chrono.ISOChronology
import org.joda.time.{
  Chronology,
  DateTime,
  DateTimeZone,
  Duration,
  LocalDate,
  LocalDateTime,
  LocalTime
}

trait JodaCoders {
  implicit def instantCoder: Coder[org.joda.time.Instant] = Coder.beam(InstantCoder.of())
  implicit def jodaDateTimeCoder: Coder[DateTime] = Coder.beam(new JodaDateTimeCoder)
  implicit def jodaLocalDateTimeCoder: Coder[LocalDateTime] = Coder.beam(new JodaLocalDateTimeCoder)
  implicit def jodaLocalDateCoder: Coder[LocalDate] = Coder.beam(new JodaLocalDateCoder)
  implicit def jodaLocalTimeCoder: Coder[LocalTime] = Coder.beam(new JodaLocalTimeCoder)
  implicit def jodaDurationCoder: Coder[Duration] =
    Coder.xmap(Coder[Long])(Duration.millis, _.getMillis)
}

object JodaCoders {
  def checkChronology(chronology: Chronology): Unit =
    if (chronology != null && chronology != ISOChronology.getInstanceUTC) {
      throw new IllegalArgumentException(s"Unsupported chronology: $chronology")
    }
}

final private class JodaDateTimeCoder extends AtomicCoder[DateTime] {
  override def encode(value: DateTime, os: OutputStream): Unit = {
    val dos = new DataOutputStream(os)

    dos.writeLong(value.getMillis)
    dos.writeUTF(value.getZone.getID)
  }

  override def decode(is: InputStream): DateTime = {
    val dis = new DataInputStream(is)

    val ms = dis.readLong()
    val zone = DateTimeZone.forID(dis.readUTF())

    new DateTime(ms, zone)
  }
}

final private class JodaLocalDateTimeCoder extends AtomicCoder[LocalDateTime] {
  import JodaCoders.checkChronology

  override def encode(value: LocalDateTime, os: OutputStream): Unit = {
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

  override def decode(is: InputStream): LocalDateTime = {
    val dis = new DataInputStream(is)

    val year = dis.readShort().toInt
    val month = dis.readShort().toInt
    val day = dis.readShort().toInt
    val hour = dis.readShort().toInt
    val minute = dis.readShort().toInt
    val second = dis.readShort().toInt
    val ms = dis.readShort().toInt

    new LocalDateTime(year, month, day, hour, minute, second, ms)
  }
}

final private class JodaLocalDateCoder extends AtomicCoder[LocalDate] {
  import JodaCoders.checkChronology

  override def encode(value: LocalDate, os: OutputStream): Unit = {
    checkChronology(value.getChronology)
    val dos = new DataOutputStream(os)

    dos.writeShort(value.getYear)
    dos.writeShort(value.getMonthOfYear)
    dos.writeShort(value.getDayOfMonth)
  }

  override def decode(is: InputStream): LocalDate = {
    val dis = new DataInputStream(is)

    val year = dis.readShort().toInt
    val month = dis.readShort().toInt
    val day = dis.readShort().toInt

    new LocalDate(year, month, day)
  }
}

final private class JodaLocalTimeCoder extends AtomicCoder[LocalTime] {
  import JodaCoders.checkChronology

  override def encode(value: LocalTime, os: OutputStream): Unit = {
    checkChronology(value.getChronology)
    val dos = new DataOutputStream(os)

    dos.writeShort(value.getHourOfDay)
    dos.writeShort(value.getMinuteOfHour)
    dos.writeShort(value.getSecondOfMinute)
    dos.writeShort(value.getMillisOfSecond)
  }

  override def decode(is: InputStream): LocalTime = {
    val dis = new DataInputStream(is)

    val hour = dis.readShort().toInt
    val minute = dis.readShort().toInt
    val second = dis.readShort().toInt
    val ms = dis.readShort().toInt

    new LocalTime(hour, minute, second, ms)
  }
}
