/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.avro

// scalafix:off
/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */

import scala.annotation.switch

case class AvroHugger(var test: Int) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(0)

  def get(field$ : Int): AnyRef = {
    (field$ : @switch) match {
      case 0 =>
        {
          test
        }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }

  def put(field$ : Int, value: Any): Unit = {
    (field$ : @switch) match {
      case 0 =>
        this.test = {
          value
        }.asInstanceOf[Int]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }

  def getSchema: org.apache.avro.Schema = AvroHugger.SCHEMA$
}

object AvroHugger {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"AvroHugger\",\"namespace\":\"com.spotify.scio.avro\",\"fields\":[{\"name\":\"test\",\"type\":\"int\"}]}"
  )
}
