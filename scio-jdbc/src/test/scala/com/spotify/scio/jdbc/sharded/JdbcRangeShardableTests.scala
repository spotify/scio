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

class JdbcRangeShardableTests extends AnyFlatSpec {

  "long shardable" must "correctly partition a range of longs" in {

    val shardable = ShardBy.range.of[Long]
    val queries = shardable.partition(Range(1, 39), 3)

    queries must contain theSameElementsAs (
      Seq(
        RangeShardQuery(Range(1, 13), upperBoundInclusive = false),
        RangeShardQuery(Range(13, 25), upperBoundInclusive = false),
        RangeShardQuery(Range(25, 39), upperBoundInclusive = true)
      )
    )
  }

  "long shardable" must "correctly partition a range of longs into a single partition" in {

    val shardable = ShardBy.range.of[Long]
    val queries = shardable.partition(Range(1, 39), 1)

    queries must contain theSameElementsAs (
      Seq(RangeShardQuery(Range(1, 39), upperBoundInclusive = true))
    )
  }

}
