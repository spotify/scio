package com.spotify.scio.examples.extra

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

import com.spotify.scio.avro.Account
import com.spotify.scio.examples.extra.ParquetExample.{AccountInput, AccountOutput}
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import com.spotify.scio.io.TextIO

class ParquetExampleTest extends PipelineSpec {

  lazy val dummyData: IndexedSeq[Account] = (1 to 100)
    .map(i =>
      Account
        .newBuilder()
        .setId(i)
        .setType(if (i % 3 == 0) "current" else "checking")
        .setName(s"account $i")
        .setAmount(i.toDouble)
        .build()
    )

  "ParquetExample" should "work for specific input" in {
    val input =
      Seq(new Account(1, "checking", "Alice", 1000.0), new Account(2, "checking", "Bob", 1500.0))

    val expected = input
      // .map(x => AccountOutput(x.getId(), x.getName.toString))
      .map(_.toString)

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--input=in.parquet", "--output=out.txt", "--method=avroSpecificIn")
      .input(ParquetAvroIO[Account]("in.parquet"), input)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(expected))
      .run()
  }

  "ParquetExample" should "work for typed input" in {
    val input =
      Seq(AccountInput(1, "checking", "Alice", 1000.0), AccountInput(2, "checking", "Bob", 1500.0))

    val expected = input.map(_.toString)

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--input=in.parquet", "--output=out.txt", "--method=typedIn")
      .input(ParquetTypeIO[AccountInput]("in.parquet"), input)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(expected))
      .run()
  }

  "ParquetExample" should "work for specific output" in {
    val expected = dummyData

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--output=out.parquet", "--method=avroOut")
      .output(ParquetAvroIO[Account]("out.parquet"))(coll =>
        coll should containInAnyOrder(expected)
      )
      .run()
  }

  "ParquetExample" should "work for typed output" in {
    val expected = dummyData
      .map(x => AccountOutput(x.getId, x.getName.toString))

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--output=out.parquet", "--method=typedOut")
      .output(ParquetTypeIO[AccountOutput]("out.parquet"))(coll =>
        coll should containInAnyOrder(expected)
      )
      .run()
  }

}
