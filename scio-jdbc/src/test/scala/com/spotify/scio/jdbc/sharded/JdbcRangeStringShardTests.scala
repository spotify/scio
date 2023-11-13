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

  "hex uuid lower shardable" must "partition a range of uuid strings in the lower case" in {
    val shard = Shard.range[UuidLowerString]
    val queries = shard.partition(
      Range(
        UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb6310"),
        UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb6337")
      ),
      3
    )

    queries must contain theSameElementsAs (
      Seq(
        RangeShardQuery(
          Range(
            UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb6310"),
            UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb631d")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb631d"),
            UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb632a")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb632a"),
            UuidLowerString("02c9cba1-eaa5-4c3d-b099-896730eb6337")
          ),
          upperBoundInclusive = true,
          quoteValues = true
        )
      )
    )
  }

  "hex uuid upper shardable" must "partition a range of uuid strings in the upper case" in {
    val shard = Shard.range[UuidUpperString]
    val queries = shard.partition(
      Range(
        UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB6310"),
        UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB6337")
      ),
      3
    )

    queries must contain theSameElementsAs (
      Seq(
        RangeShardQuery(
          Range(
            UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB6310"),
            UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB631D")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB631D"),
            UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB632A")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB632A"),
            UuidUpperString("02C9CBA1-EAA5-4C3D-B099-896730EB6337")
          ),
          upperBoundInclusive = true,
          quoteValues = true
        )
      )
    )
  }

  "hex sqlserver uuid upper shardable" must "partition a range of uuid strings in the upper case" in {
    val shard = Shard.range[SqlServerUuidUpperString]
    val queries = shard.partition(
      Range(
        SqlServerUuidUpperString("00000000-0000-0000-0000-000000000000"),
        SqlServerUuidUpperString("00000000-0000-0000-0000-000000000001")
      ),
      3
    )

    queries must contain theSameElementsAs (
      Seq(
        RangeShardQuery(
          Range(
            SqlServerUuidUpperString("00000000-0000-0000-0000-000000000000"),
            SqlServerUuidUpperString("55555555-5555-5555-5555-000000000000")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            SqlServerUuidUpperString("55555555-5555-5555-5555-000000000000"),
            SqlServerUuidUpperString("AAAAAAAA-AAAA-AAAA-AAAA-000000000000")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            SqlServerUuidUpperString("AAAAAAAA-AAAA-AAAA-AAAA-000000000000"),
            SqlServerUuidUpperString("00000000-0000-0000-0000-000000000001")
          ),
          upperBoundInclusive = true,
          quoteValues = true
        )
      )
    )
  }

  "hex sqlserver uuid lower shardable" must "partition a range of uuid strings in the upper case" in {
    val shard = Shard.range[SqlServerUuidLowerString]
    val queries = shard.partition(
      Range(
        SqlServerUuidLowerString("00000000-0000-0000-0000-000000000001"),
        SqlServerUuidLowerString("00000000-0000-0000-0000-100000000000")
      ),
      3
    )

    queries must contain theSameElementsAs (
      Seq(
        RangeShardQuery(
          Range(
            SqlServerUuidLowerString("00000000-0000-0000-0000-000000000000"),
            SqlServerUuidLowerString("00000000-0000-0000-0000-055555555556")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            SqlServerUuidLowerString("00000000-0000-0000-0000-055555555556"),
            SqlServerUuidLowerString("00000000-0000-0000-0000-0aaaaaaaaaab")
          ),
          upperBoundInclusive = false,
          quoteValues = true
        ),
        RangeShardQuery(
          Range(
            SqlServerUuidLowerString("00000000-0000-0000-0000-0aaaaaaaaaab"),
            SqlServerUuidLowerString("00000000-0000-0000-0000-100000000000")
          ),
          upperBoundInclusive = true,
          quoteValues = true
        )
      )
    )
  }
}
