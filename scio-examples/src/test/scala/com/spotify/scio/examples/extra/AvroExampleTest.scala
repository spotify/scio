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

package com.spotify.scio.examples.extra

import com.spotify.scio.avro._
import com.spotify.scio.examples.extra.AvroExample.{AccountFromSchema, AccountToSchema}
import com.spotify.scio.io._
import com.spotify.scio.testing._

class AvroExampleTest extends PipelineSpec {

  "AvroExample" should "work for specific input" in {
    val input = Seq(
      new Account(1, "checking", "Alice", 1000.0),
      new Account(2, "checking", "Bob", 1500.0))

    val expected = input.map(_.toString)

    JobTest[com.spotify.scio.examples.extra.AvroExample.type]
      .args("--input=in.avro", "--output=out.txt", "--method=specificIn")
      .input(AvroIO("in.avro"), input)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

  it should "work for specific output" in {
    val expected = (1 to 100)
      .map { i =>
        Account.newBuilder()
          .setId(i)
          .setAmount(i.toDouble)
          .setName("account" + i)
          .setType("checking")
          .build()
      }

    JobTest[com.spotify.scio.examples.extra.AvroExample.type]
      .args("--output=out.avro", "--method=specificOut")
      .output(AvroIO[Account]("out.avro"))(_ should containInAnyOrder (expected))
      .run()
  }

  "AvroExample" should "work for typed input" in {
    val input = Seq(
      AccountFromSchema(1, "checking", "Alice", 1000.0),
      AccountFromSchema(2, "checking", "Bob", 1500.0))

    val expected = input.map(_.toString)

    JobTest[com.spotify.scio.examples.extra.AvroExample.type]
      .args("--input=in.avro", "--output=out.txt", "--method=typedIn")
      .input(AvroIO("in.avro"), input)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

  it should "work for typed output" in {
    val expected = (1 to 100).map { i =>
      AccountToSchema(id = i,
        amount = i.toDouble,
        name = "account" + i,
        `type` = "checking")
    }

    JobTest[com.spotify.scio.examples.extra.AvroExample.type]
      .args("--output=out.avro", "--method=typedOut")
      .output(AvroIO[AccountToSchema]("out.avro"))(_ should containInAnyOrder (expected))
      .run()
  }

}
