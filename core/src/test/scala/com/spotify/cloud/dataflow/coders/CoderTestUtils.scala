package com.spotify.cloud.dataflow.coders

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.cloud.dataflow.avro.TestRecord
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._

object CoderTestUtils {

  case class Pair(name: String, size: Int)
  case class CaseClassWithGenericRecord(name: String, size: Int, record: GenericRecord)
  case class CaseClassWithSpecificRecord(name: String, size: Int, record: TestRecord)

  def testRoundTrip[T](coder: Coder[T], value: T): Boolean = {
    val bytes = CoderUtils.encodeToByteArray(coder, value)
    val result = CoderUtils.decodeFromByteArray(coder, bytes)
    result == value
  }

  def newGenericRecord: GenericRecord = {
    def f(name: String, tpe: Schema.Type) =
      new Schema.Field(
        name,
        Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(tpe)).asJava),
        null, null)

    val schema = Schema.createRecord(List(
      f("int_field", Schema.Type.INT),
      f("long_field", Schema.Type.LONG),
      f("float_field", Schema.Type.FLOAT),
      f("double_field", Schema.Type.DOUBLE),
      f("boolean_field", Schema.Type.BOOLEAN),
      f("string_field", Schema.Type.STRING)
    ).asJava)

    val r = new GenericData.Record(schema)
    r.put("int_field", 1)
    r.put("long_field", 1L)
    r.put("float_field", 1F)
    r.put("double_field", 1.0)
    r.put("boolean_field", true)
    r.put("string_field", "hello")
    r
  }

}
