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

  override def toString: String = value
}

object ShardString {

  sealed trait HexString extends ShardString
  final case class HexUpperString(value: String) extends HexString
  final case class HexLowerString(value: String) extends HexString

  sealed trait UuidString extends HexString
  final case class UuidLowerString(value: String) extends UuidString
  final case class UuidUpperString(value: String) extends UuidString

  sealed trait SqlServerUuidString extends HexString
  final case class SqlServerUuidUpperString(value: String) extends SqlServerUuidString
  final case class SqlServerUuidLowerString(value: String) extends SqlServerUuidString

  final case class Base64String(value: String) extends ShardString

}

trait RangeShardStringCodec[T <: ShardString] extends Serializable {
  def encode(bigInt: BigInt): T
  def decode(str: T): BigInt
  def lift(str: String): T
}

object RangeShardStringCodec {
  import ShardString._

  private def hexShardRangeStringCodec[S <: HexString](
    hexStringBuilder: String => S
  ): RangeShardStringCodec[S] =
    new RangeShardStringCodec[S] {
      def encode(bigInt: BigInt): S = lift(bigInt.toString(16))
      def decode(str: S): BigInt = BigInt(str.value, 16)
      def lift(str: String): S = hexStringBuilder(str)
    }

  implicit val hexUpperShardRangeStringCodec: RangeShardStringCodec[HexUpperString] =
    hexShardRangeStringCodec(str => HexUpperString(str.toUpperCase))

  implicit val hexLowerShardRangeStringCodec: RangeShardStringCodec[HexLowerString] =
    hexShardRangeStringCodec(str => HexLowerString(str.toLowerCase))

  implicit def uuidShardRangeStringCodec[S <: UuidString](
    uuidStringBuilder: String => S
  ): RangeShardStringCodec[S] =
    new RangeShardStringCodec[S] {
      def encode(bigInt: BigInt): S = lift(f"$bigInt%032x")
      def decode(str: S): BigInt = BigInt(str.value.replaceAll("-", ""), 16)
      def lift(str: String): S = uuidStringBuilder(
        Seq(
          str.substring(0, 8),
          str.substring(8, 12),
          str.substring(12, 16),
          str.substring(16, 20),
          str.substring(20)
        ).mkString("-")
      )
    }

  implicit val uuidLowerShardRangeStringCodec: RangeShardStringCodec[UuidLowerString] =
    uuidShardRangeStringCodec(str => UuidLowerString(str.toLowerCase))

  implicit val uuidUpperShardRangeStringCodec: RangeShardStringCodec[UuidUpperString] =
    uuidShardRangeStringCodec(str => UuidUpperString(str.toUpperCase))

  implicit val base64ShardRangeStringCodec: RangeShardStringCodec[Base64String] =
    new RangeShardStringCodec[Base64String] {
      def encode(bigInt: BigInt): Base64String =
        lift(new String(Base64.encodeInteger(bigInt.bigInteger), StandardCharsets.UTF_8))
      def decode(str: Base64String): BigInt =
        Base64.decodeInteger(str.value.getBytes(StandardCharsets.UTF_8))
      def lift(str: String): Base64String = Base64String(str)
    }

  implicit def sqlServerUuidShardRangeStringCodec[S <: SqlServerUuidString](
    uuidStringBuilder: String => S
  ): RangeShardStringCodec[S] =
    new RangeShardStringCodec[S] {

      import java.util.UUID

      def encode(bigInt: BigInt): S = {
        val bytes = Array.tabulate(16) { i =>
          ((bigInt >> (i * 8)) & 0xff).toByte
        }

        val uuidBytes = Array(
          bytes(0),
          bytes(1),
          bytes(2),
          bytes(3),
          bytes(4),
          bytes(5),
          bytes(6),
          bytes(7),
          bytes(9),
          bytes(8),
          bytes(15),
          bytes(14),
          bytes(13),
          bytes(12),
          bytes(11),
          bytes(10)
        )

        val uuid = new UUID(
          java.nio.ByteBuffer.wrap(uuidBytes, 0, 8).getLong,
          java.nio.ByteBuffer.wrap(uuidBytes, 8, 8).getLong
        )

        lift(uuid.toString)
      }

      def decode(str: S): BigInt = {
        val formattedStr = str.value.replace("-", "")
        val bytes =
          formattedStr.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

        val reversedBytes = Array(
          bytes(0),
          bytes(1),
          bytes(2),
          bytes(3),
          bytes(4),
          bytes(5),
          bytes(6),
          bytes(7),
          bytes(9),
          bytes(8),
          bytes(15),
          bytes(14),
          bytes(13),
          bytes(12),
          bytes(11),
          bytes(10)
        )

        val bigIntValue = reversedBytes.zipWithIndex.map { case (byte, index) =>
          BigInt(byte & 0xff) << (index * 8)
        }.sum

        bigIntValue
      }

      def lift(str: String): S = uuidStringBuilder(str)
    }

  implicit val sqlServerUuidLowerShardRangeStringCodec
    : RangeShardStringCodec[SqlServerUuidLowerString] =
    sqlServerUuidShardRangeStringCodec(str => SqlServerUuidLowerString(str.toLowerCase))

  implicit val sqlServerUuidUpperShardRangeStringCodec
    : RangeShardStringCodec[SqlServerUuidUpperString] =
    sqlServerUuidShardRangeStringCodec(str => SqlServerUuidUpperString(str.toUpperCase))

}
