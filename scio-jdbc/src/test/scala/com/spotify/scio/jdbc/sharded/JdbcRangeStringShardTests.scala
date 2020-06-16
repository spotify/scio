/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.jdbc.sharded

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers._
import ShardString._

class JdbcRangeStringShardTests extends AnyFlatSpec {

  "hex upper shardable" must "correctly partition a range of hex strings in the upper case" in {

    val shard = Shard.range[HexUpperString]
    val queries = shard.partition(Range(HexUpperString("1"), HexUpperString("27")), 3)

    queries must contain theSameElementsAs (
      Seq(
        RangeShardQuery(
          Range(HexUpperString("1"), HexUpperString("D")),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(HexUpperString("D"), HexUpperString("19")),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(HexUpperString("19"), HexUpperString("27")),
          upperBoundInclusive = true,
          quoteValues = true
        )
      )
    )
  }

  "base64 shardable" must "correctly partition a range of base64 strings" in {

    val shard = Shard.range[Base64String]
    val queries = shard.partition(Range(Base64String("AQ=="), Base64String("Jw==")), 3)

    queries must contain theSameElementsAs (
      Seq(
        RangeShardQuery(
          Range(Base64String("AQ=="), Base64String("DQ==")),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(Base64String("DQ=="), Base64String("GQ==")),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(Base64String("GQ=="), Base64String("Jw==")),
          upperBoundInclusive = true,
          quoteValues = true
        )
      )
    )
  }
}
