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

import com.spotify.scio.jdbc.sharded.ShardString.{HexLowerString, HexUpperString}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers._

class ShardedQueryTests extends AnyFlatSpec {

  "toSelectStatement" must "produce the correct statement when upper bound is excluded" in {

    val shardQuery =
      RangeShardQuery[Long](Range(1, 9), upperBoundInclusive = false, quoteValues = false)

    ShardQuery.toSelectStatement(shardQuery, "t", "c") mustBe
      "SELECT * FROM t WHERE c >= 1 and c < 9"
  }

  "toSelectStatement" must "produce the correct statement when upper bound is included" in {

    val shardQuery =
      RangeShardQuery[Long](Range(1, 9), upperBoundInclusive = true, quoteValues = false)

    ShardQuery.toSelectStatement(shardQuery, "t", "c") mustBe
      "SELECT * FROM t WHERE c >= 1 and c <= 9"
  }

  "toSelectStatement" must "produce the correct statement for a prefix query" in {

    val shardQuery = PrefixShardQuery("abc")

    ShardQuery.toSelectStatement(shardQuery, "t", "c") mustBe
      "SELECT * FROM t WHERE c LIKE 'abc%'"
  }

  "toSelectStatement" must "produce the correct statement for hex string in upper case" in {

    val shardQuery = RangeShardQuery[HexUpperString](
      Range(HexUpperString("a"), HexUpperString("f")),
      upperBoundInclusive = true,
      quoteValues = true
    )

    ShardQuery.toSelectStatement(shardQuery, "t", "c") mustBe
      "SELECT * FROM t WHERE c >= 'A' and c <= 'F'"
  }

  "toSelectStatement" must "produce the correct statement for hex string in lower case" in {

    val shardQuery = RangeShardQuery[HexLowerString](
      Range(HexLowerString("a"), HexLowerString("f")),
      upperBoundInclusive = true,
      quoteValues = true
    )

    ShardQuery.toSelectStatement(shardQuery, "t", "c") mustBe
      "SELECT * FROM t WHERE c >= 'a' and c <= 'f'"
  }

}
