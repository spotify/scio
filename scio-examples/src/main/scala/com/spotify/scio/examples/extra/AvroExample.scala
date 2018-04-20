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

package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.avro.Account
import com.spotify.scio.avro.types.AvroType
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._

// Read and write specific and generic Avro records
object AvroExample {
  @AvroType.fromSchema(
    """{
      | "type":"record",
      | "name":"Account",
      | "namespace":"com.spotify.scio.avro",
      | "doc":"Record for an account",
      | "fields":[
      |   {"name":"id","type":"int"},
      |   {"name":"type","type":"string"},
      |   {"name":"name","type":"string"},
      |   {"name":"amount","type":"double"}]}
    """.stripMargin)
  class AccountFromSchema

  @AvroType.toSchema
  case class AccountToSchema(id: Int, `type`: String, name: String, amount: Double)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val m = args("method")
    m match {
      // write dummy specific records
      case "specificOut" => specificOut(sc, args)

      // read dummy specific records
      case "specificIn" => specificIn(sc, args)

      // write dummy generic records
      case "genericOut" => genericOut(sc, args)

      // read dummy generic records
      case "genericIn" => genericIn(sc, args)

      // write typed generic records
      case "typedOut" => typedOut(sc, args)

      // read typed generic records
      case "typedIn" => typedIn(sc, args)

      case _ => throw new RuntimeException(s"Invalid method $m")
    }

    sc.close()
  }

  private def specificOut(sc: ScioContext, args: Args): Unit = {
    sc.parallelize(1 to 100)
      .map { i =>
        Account.newBuilder()
          .setId(i)
          .setAmount(i.toDouble)
          .setName("account" + i)
          .setType("checking")
          .build()
      }
      .saveAsAvroFile(args("output"))
  }

  private def specificIn(sc: ScioContext, args: Args): Unit = {
    sc.avroFile[Account](args("input"))
      .map(_.toString)
      .saveAsTextFile(args("output"))
  }

  private def genericOut(sc: ScioContext, args: Args): Unit = {
    // Schema is not serializable and breaks lambda when pulled in from closure
    val schemaString = schema.toString
    sc.parallelize(1 to 100)
      .map { i =>
        val s: Schema = new Parser().parse(schemaString)
        val r = new GenericData.Record(s)
        r.put("id", i)
        r.put("amount", i.toDouble)
        r.put("name", "account" + i)
        r.put("type", "checking")
        r
      }
      .saveAsAvroFile(args("output"), schema = schema)
  }

  private def typedIn(sc: ScioContext, args: Args): Unit = {
    sc.typedAvroFile[AccountFromSchema](args("input"))
      .saveAsTextFile(args("output"))
  }

  private def typedOut(sc: ScioContext, args: Args): Unit = {
    sc.parallelize(1 to 100)
      .map { i =>
        AccountToSchema(id = i,
          amount = i.toDouble,
          name = "account" + i,
          `type` = "checking") }
      .saveAsTypedAvroFile(args("output"))
  }

  private def genericIn(sc: ScioContext, args: Args): Unit = {
    sc.avroFile[GenericRecord](args("input"), schema)
      .map(_.toString)
      .saveAsTextFile(args("output"))
  }

  val schema = {
    def f(name: String, tpe: Schema.Type) =
      new Schema.Field(
        name,
        Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(tpe)).asJava),
        null: String, null: AnyRef)

    val s = Schema.createRecord("GenericAccountRecord", null, null, false)
    s.setFields(List(
      f("id", Schema.Type.INT),
      f("amount", Schema.Type.DOUBLE),
      f("name", Schema.Type.STRING),
      f("type", Schema.Type.STRING)
    ).asJava)
    s
  }

}
