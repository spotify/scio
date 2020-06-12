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

import java.nio.charset.StandardCharsets
import org.apache.commons.codec.binary.Base64

sealed trait ShardString extends Serializable {
  val value: String
}

object ShardString {

  final case class HexUpperString(value: String) extends ShardString {
    override def toString: String = value.toUpperCase
  }

  final case class HexLowerString(value: String) extends ShardString {
    override def toString: String = value.toLowerCase
  }

  final case class Base64String(value: String) extends ShardString

}

trait RangeShardStringCodec[T <: ShardString] extends Serializable {
  def encode(bigInt: BigInt): T
  def decode(str: T): BigInt
  def lift(str: String): T
}

object RangeShardStringCodec {
  import ShardString._

  implicit val hexUpperShardRangeStringCodec: RangeShardStringCodec[HexUpperString] =
    new RangeShardStringCodec[HexUpperString] {
      def encode(bigInt: BigInt): HexUpperString = lift(bigInt.toString(16).toUpperCase)
      def decode(str: HexUpperString): BigInt = BigInt(str.value, 16)
      def lift(str: String): HexUpperString = HexUpperString(str)
    }

  implicit val hexLowerShardRangeStringCodec: RangeShardStringCodec[HexLowerString] =
    new RangeShardStringCodec[HexLowerString] {
      def encode(bigInt: BigInt): HexLowerString = lift(bigInt.toString(16).toLowerCase)
      def decode(str: HexLowerString): BigInt = BigInt(str.value, 16)
      def lift(str: String): HexLowerString = HexLowerString(str)
    }

  implicit val base64ShardRangeStringCodec: RangeShardStringCodec[Base64String] =
    new RangeShardStringCodec[Base64String] {
      def encode(bigInt: BigInt): Base64String =
        lift(new String(Base64.encodeInteger(bigInt.bigInteger), StandardCharsets.UTF_8))
      def decode(str: Base64String): BigInt =
        Base64.decodeInteger(str.value.getBytes(StandardCharsets.UTF_8))
      def lift(str: String): Base64String = Base64String(str)
    }

}
