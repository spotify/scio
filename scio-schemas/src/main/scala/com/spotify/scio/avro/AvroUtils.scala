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

package com.spotify.scio.avro

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.annotation.switch
import scala.collection.JavaConverters._

object AvroUtils {

  private def f(name: String, tpe: Schema.Type) =
    new Schema.Field(
      name,
      Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(tpe)).asJava),
      null: String,
      null: AnyRef)

  private def fArr(name: String, tpe: Schema.Type) =
    new Schema.Field(name, Schema.createArray(Schema.create(tpe)), null: String, null: AnyRef)

  val schema = Schema.createRecord("GenericTestRecord", null, null, false)
  schema.setFields(
    List(
      f("int_field", Schema.Type.INT),
      f("long_field", Schema.Type.LONG),
      f("float_field", Schema.Type.FLOAT),
      f("double_field", Schema.Type.DOUBLE),
      f("boolean_field", Schema.Type.BOOLEAN),
      f("string_field", Schema.Type.STRING),
      fArr("array_field", Schema.Type.STRING)
    ).asJava)

  def newGenericRecord(i: Int): GenericRecord = {
    val r = new GenericData.Record(schema)
    r.put("int_field", 1 * i)
    r.put("long_field", 1L * i)
    r.put("float_field", 1F * i)
    r.put("double_field", 1.0 * i)
    r.put("boolean_field", true)
    r.put("string_field", "hello")
    r.put("array_field", List[CharSequence]("a", "b", "c").asJava)
    r
  }

  def newSpecificRecord(i: Int): TestRecord =
    new TestRecord(i,
                   i.toLong,
                   i.toFloat,
                   i.toDouble,
                   true,
                   "hello",
                   List[CharSequence]("a", "b", "c").asJava)

  def newCaseClassSpecificRecord(i: Int): CaseClassTestRecord =
    new CaseClassTestRecord(Option(i),
                            Option(i.toLong),
                            Option(i.toFloat),
                            Option(i.toDouble),
                            Option(true),
                            Option("hello"),
                            List[String]("a", "b", "c"))

}

// scalastyle:off
case class CaseClassTestRecord(var int_field: Option[Int] = None,
                               var long_field: Option[Long] = None,
                               var float_field: Option[Float] = None,
                               var double_field: Option[Double] = None,
                               var boolean_field: Option[Boolean] = None,
                               var string_field: Option[String] = None,
                               var array_field: Seq[String] = Seq())
    extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(None, None, None, None, None, None, Seq())
  def get(field$ : Int): AnyRef = {
    (field$ : @switch) match {
      case 0 => {
        int_field match {
          case Some(x) => x
          case None    => null
        }
      }.asInstanceOf[AnyRef]
      case 1 => {
        long_field match {
          case Some(x) => x
          case None    => null
        }
      }.asInstanceOf[AnyRef]
      case 2 => {
        float_field match {
          case Some(x) => x
          case None    => null
        }
      }.asInstanceOf[AnyRef]
      case 3 => {
        double_field match {
          case Some(x) => x
          case None    => null
        }
      }.asInstanceOf[AnyRef]
      case 4 => {
        boolean_field match {
          case Some(x) => x
          case None    => null
        }
      }.asInstanceOf[AnyRef]
      case 5 => {
        string_field match {
          case Some(x) => x
          case None    => null
        }
      }.asInstanceOf[AnyRef]
      case 6 => {
        scala.collection.JavaConverters
          .bufferAsJavaListConverter({
            array_field map { x =>
              x
            }
          }.toBuffer)
          .asJava
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$ : Int, value: Any): Unit = {
    (field$ : @switch) match {
      case 0 =>
        this.int_field = {
          value match {
            case null => None
            case _    => Some(value)
          }
        }.asInstanceOf[Option[Int]]
      case 1 =>
        this.long_field = {
          value match {
            case null => None
            case _    => Some(value)
          }
        }.asInstanceOf[Option[Long]]
      case 2 =>
        this.float_field = {
          value match {
            case null => None
            case _    => Some(value)
          }
        }.asInstanceOf[Option[Float]]
      case 3 =>
        this.double_field = {
          value match {
            case null => None
            case _    => Some(value)
          }
        }.asInstanceOf[Option[Double]]
      case 4 =>
        this.boolean_field = {
          value match {
            case null => None
            case _    => Some(value)
          }
        }.asInstanceOf[Option[Boolean]]
      case 5 =>
        this.string_field = {
          value match {
            case null => None
            case _    => Some(value.toString)
          }
        }.asInstanceOf[Option[String]]
      case 6 =>
        this.array_field = {
          value match {
            case (array: java.util.List[_]) => {
              Seq(
                scala.collection.JavaConverters
                  .asScalaIteratorConverter(array.iterator)
                  .asScala
                  .toSeq map { x =>
                  x.toString
                }: _*)
            }
          }
        }.asInstanceOf[Seq[String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = CaseClassTestRecord.SCHEMA$
}

object CaseClassTestRecord {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\"," +
    "\"name\":\"CaseClassTestRecord\",\"namespace\":\"com.spotify.scio.avro\",\"doc\":\"Record" +
    " for testing\",\"fields\":[{\"name\":\"int_field\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"long_field\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"float_field\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"double_field\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"boolean_field\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"string_field\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"array_field\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":null}]}")
}

// scalastyle:on
