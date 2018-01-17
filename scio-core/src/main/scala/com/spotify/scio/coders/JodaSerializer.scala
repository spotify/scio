/*
 * Copyright 2017 Spotify AB.
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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.joda.time.{LocalDate, LocalDateTime, LocalTime, DateTime, DateTimeZone}
import org.joda.time.chrono.ISOChronology

private class JodaLocalDateTimeSerializer extends Serializer[LocalDateTime] {
  setImmutable(true)

  def write(kryo: Kryo, output: Output, ldt: LocalDateTime): Unit = {
    output.writeInt(ldt.getYear, /*optimizePositive=*/ false)
    output.writeByte(ldt.getMonthOfYear)
    output.writeByte(ldt.getDayOfMonth)
    output.writeLong(ldt.getMillisOfDay)

    val chronology = ldt.getChronology
    if (chronology != null && chronology != ISOChronology.getInstanceUTC) {
      sys.error(s"Unsupported chronology: $chronology")
    }
  }

  def read(kryo: Kryo, input: Input, tpe: Class[LocalDateTime]): LocalDateTime = {
    val year = input.readInt(/*optimizePositive=*/ false)
    val month = input.readByte().toInt
    val day = input.readByte().toInt

    val millis = input.readLong()
    val hour = (millis / 3600000L).toInt
    val minute = ((millis % 3600000L) / 60000).toInt
    val second = ((millis % 60000L) / 1000).toInt
    val ms = (millis % 1000L).toInt

    new LocalDateTime(year, month, day, hour, minute, second, ms)
  }
}

private class JodaLocalTimeSerializer extends Serializer[LocalTime] {
  setImmutable(true)

  def write(kryo: Kryo, output: Output, lt: LocalTime): Unit = {
    output.writeInt(lt.getMillisOfDay, /*optimizePositive=*/ false)

    val chronology = lt.getChronology
    if (chronology != null && chronology != ISOChronology.getInstanceUTC) {
      sys.error(s"Unsupported chronology: $chronology")
    }
  }

  def read(kryo: Kryo, input: Input, tpe: Class[LocalTime]): LocalTime = {
    LocalTime.fromMillisOfDay(input.readInt(/*optimizePositive=*/ false))
  }
}


private class JodaLocalDateSerializer extends Serializer[LocalDate] {
  setImmutable(true)

  def write(kryo: Kryo, output: Output, ld: LocalDate): Unit = {
    output.writeInt(ld.getYear, /*optimizePositive=*/ false)
    output.writeByte(ld.getMonthOfYear)
    output.writeByte(ld.getDayOfMonth)

    val chronology = ld.getChronology
    if (chronology != null && chronology != ISOChronology.getInstanceUTC) {
      sys.error(s"Unsupported chronology: $chronology")
    }
  }

  def read(kryo: Kryo, input: Input, tpe: Class[LocalDate]): LocalDate = {
    val year = input.readInt(/*optimizePositive=*/ false)
    val month = input.readByte().toInt
    val day = input.readByte().toInt

    new LocalDate(year, month, day)
  }
}

class JodaDateTimeSerializer extends Serializer[DateTime] {
  setImmutable(true)

  def write(kryo: Kryo, output: Output, dt: DateTime): Unit = {
    output.writeLong(dt.getMillis)
    output.writeString(dt.getZone.getID)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[DateTime]): DateTime = {
    val millis = input.readLong()
    val zone = DateTimeZone.forID(input.readString())
    new DateTime(millis,zone)
  }
}
