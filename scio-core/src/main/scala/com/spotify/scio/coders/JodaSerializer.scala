package com.spotify.scio.coders

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.joda.time.{LocalDate, LocalDateTime}
import org.joda.time.chrono.ISOChronology

class JodaLocalDateTimeSerializer extends Serializer[LocalDateTime] {
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

class JodaLocalDateSerializer extends Serializer[LocalDate] {
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
