package com.spotify.scio.testing

import java.lang.{Iterable => JIterable}

import com.google.common.collect.Lists
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.bigquery.TableRow
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._

private[scio] object TestingUtils {

  // Value type Iterable[T] is wrapped from Java and fails equality check
  def iterable[T](elems: T*): Iterable[T] = Lists.newArrayList(elems: _*).asInstanceOf[JIterable[T]].asScala

  def newGenericRecord(i: Int): GenericRecord = {
    def f(name: String, tpe: Schema.Type) =
      new Schema.Field(
        name,
        Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(tpe)).asJava),
        null, null)

    val schema = Schema.createRecord("GenericTestRecord", null, null, false)
    schema.setFields(List(
      f("int_field", Schema.Type.INT),
      f("long_field", Schema.Type.LONG),
      f("float_field", Schema.Type.FLOAT),
      f("double_field", Schema.Type.DOUBLE),
      f("boolean_field", Schema.Type.BOOLEAN),
      f("string_field", Schema.Type.STRING)
    ).asJava)

    val r = new GenericData.Record(schema)
    r.put("int_field", 1 * i)
    r.put("long_field", 1L * i)
    r.put("float_field", 1F * i)
    r.put("double_field", 1.0 * i)
    r.put("boolean_field", true)
    r.put("string_field", "hello")
    r
  }

  def newSpecificRecord(i: Int): TestRecord = new TestRecord(i, i.toLong, i.toFloat, i.toDouble, true, "hello")

  def newTableRow(i: Int): TableRow = TableRow(
    "int_field" -> 1 * i,
    "long_field" -> 1L * i,
    "float_field" -> 1F * i,
    "double_field" -> 1.0 * i,
    "boolean_field" -> "true",
    "string_field" -> "hello")

}
