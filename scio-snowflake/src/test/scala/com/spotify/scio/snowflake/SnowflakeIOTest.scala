/*
 * Copyright 2024 Spotify AB
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

package com.spotify.scio.snowflake

import com.spotify.scio.testing.ScioIOSpec
import kantan.csv.RowCodec

object SnowflakeIOTest {
  final case class Data(value: String)
}

class SnowflakeIOTest extends ScioIOSpec {

  import SnowflakeIOTest._

  val connectionOptions = SnowflakeConnectionOptions(
    url = "jdbc:snowflake://host.snowflakecomputing.com"
  )

  implicit val rowCodecData: RowCodec[Data] = RowCodec.caseCodec(Data.apply)(Data.unapply)

  "SnowflakeIO" should "support query input" in {
    val input = Seq(Data("a"), Data("b"), Data("c"))
    val query = "SELECT * FROM table"
    testJobTestInput(input, query)(SnowflakeIO(connectionOptions, _))(
      _.snowflakeQuery(connectionOptions, _, "storage-integration")
    )
  }

  it should "support table input" in {
    val input = Seq(Data("a"), Data("b"), Data("c"))
    val table = "table"
    testJobTestInput(input, table)(SnowflakeIO(connectionOptions, _))(
      _.snowflakeTable(connectionOptions, _, "storage-integration")
    )
  }

  it should "support table output" in {
    val output = Seq(Data("a"), Data("b"), Data("c"))
    val table = "table"
    testJobTestOutput(output, table)(SnowflakeIO(connectionOptions, _))(
      _.saveAsSnowflake(connectionOptions, _)
    )
  }
}
