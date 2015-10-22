package com.spotify.scio.coders

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.avro.TestRecord
import org.apache.avro.generic.GenericRecord

object CoderTestUtils {

  case class Pair(name: String, size: Int)
  case class CaseClassWithGenericRecord(name: String, size: Int, record: GenericRecord)
  case class CaseClassWithSpecificRecord(name: String, size: Int, record: TestRecord)

  def testRoundTrip[T](coder: Coder[T], value: T): Boolean = testRoundTrip(coder, coder, value)

  def testRoundTrip[T](writer: Coder[T], reader: Coder[T], value: T): Boolean = {
    val bytes = CoderUtils.encodeToByteArray(writer, value)
    val result = CoderUtils.decodeFromByteArray(reader, bytes)
    result == value
  }

}
