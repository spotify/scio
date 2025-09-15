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

package com.spotify.scio.jdbc.sharded

import com.spotify.scio.jdbc.sharded.ShardString.{
  SqlServerUuidLowerString,
  SqlServerUuidUpperString
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ShardStringTest extends AnyFlatSpec with Matchers {

  "sqlServerUuid" should "encode and decode" in {
    val uuid = UUID.randomUUID()

    {
      import RangeShardStringCodec.sqlServerUuidLowerShardRangeStringCodec._
      val expected = SqlServerUuidLowerString(uuid.toString.toLowerCase)
      val actual = encode(decode(expected))
      actual shouldBe expected
    }

    {
      import RangeShardStringCodec.sqlServerUuidUpperShardRangeStringCodec._
      val expected = SqlServerUuidUpperString(uuid.toString.toUpperCase)
      val actual = encode(decode(expected))
      actual shouldBe expected
    }
  }

  it should "respect Sql Server ordering" in {
    val sortedSqlGuids = List(
      // 1st byte group of 4 bytes sorted in the reverse (left-to-right) order below
      "01000000-0000-0000-0000-000000000000",
      "10000000-0000-0000-0000-000000000000",
      "00010000-0000-0000-0000-000000000000",
      "00100000-0000-0000-0000-000000000000",
      "00000100-0000-0000-0000-000000000000",
      "00001000-0000-0000-0000-000000000000",
      "00000001-0000-0000-0000-000000000000",
      "00000010-0000-0000-0000-000000000000",
      // 2nd byte group of 2 bytes sorted in the reverse (left-to-right) order below
      "00000000-0100-0000-0000-000000000000",
      "00000000-1000-0000-0000-000000000000",
      "00000000-0001-0000-0000-000000000000",
      "00000000-0010-0000-0000-000000000000",
      // 3rd byte group of 2 bytes sorted in the reverse (left-to-right) order below
      "00000000-0000-0100-0000-000000000000",
      "00000000-0000-1000-0000-000000000000",
      "00000000-0000-0001-0000-000000000000",
      "00000000-0000-0010-0000-000000000000",
      // 4th byte group of 2 bytes sorted in the straight (right-to-left) order below
      "00000000-0000-0000-0001-000000000000",
      "00000000-0000-0000-0010-000000000000",
      "00000000-0000-0000-0100-000000000000",
      "00000000-0000-0000-1000-000000000000",
      // 5th byte group of 6 bytes sorted in the straight (right-to-left) order below
      "00000000-0000-0000-0000-000000000001",
      "00000000-0000-0000-0000-000000000010",
      "00000000-0000-0000-0000-000000000100",
      "00000000-0000-0000-0000-000000001000",
      "00000000-0000-0000-0000-000000010000",
      "00000000-0000-0000-0000-000000100000",
      "00000000-0000-0000-0000-000001000000",
      "00000000-0000-0000-0000-000010000000",
      "00000000-0000-0000-0000-000100000000",
      "00000000-0000-0000-0000-001000000000",
      "00000000-0000-0000-0000-010000000000",
      "00000000-0000-0000-0000-100000000000"
    ).map(SqlServerUuidLowerString)

    val encoded =
      sortedSqlGuids.map(RangeShardStringCodec.sqlServerUuidLowerShardRangeStringCodec.decode)
    encoded shouldBe sorted
  }
}
