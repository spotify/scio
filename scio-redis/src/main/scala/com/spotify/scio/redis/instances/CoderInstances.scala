/*
 * Copyright 2020 Spotify AB
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

package com.spotify.scio.redis.instances

import com.spotify.scio.coders.Coder
import com.spotify.scio.redis.types._

trait CoderInstances {

  // TODO: scala3 - workaround https://github.com/lampepfl/dotty/issues/9985
  implicit def appendCoder[T: Coder: RedisType]: Coder[Append[T]] = ??? // Coder.gen[Append[T]]
  implicit def setCoder[T: Coder: RedisType]: Coder[Set[T]] = ??? // Coder.gen[Set[T]]
  implicit def incrByCoder[T: Coder: RedisType]: Coder[IncrBy[T]] = ??? // Coder.gen[IncrBy[T]]
  implicit def decrByCoder[T: Coder: RedisType]: Coder[DecrBy[T]] = ??? // Coder.gen[DecrBy[T]]
  implicit def sAddCoder[T: Coder: RedisType]: Coder[SAdd[T]] = ??? // Coder.gen[SAdd[T]]
  implicit def lPushCoder[T: Coder: RedisType]: Coder[LPush[T]] = ??? // Coder.gen[LPush[T]]
  implicit def rPushCoder[T: Coder: RedisType]: Coder[RPush[T]] = ??? // Coder.gen[RPush[T]]
  implicit def pfAddCoder[T: Coder: RedisType]: Coder[PFAdd[T]] = ??? // Coder.gen[PFAdd[T]]

  private[this] def coders: Map[Int, Coder[_]] = Map(
    1 -> appendCoder[String],
    2 -> appendCoder[Array[Byte]],
    3 -> setCoder[String],
    4 -> setCoder[Array[Byte]],
    5 -> incrByCoder[String],
    6 -> incrByCoder[Array[Byte]],
    7 -> decrByCoder[String],
    8 -> decrByCoder[Array[Byte]],
    9 -> sAddCoder[String],
    10 -> sAddCoder[Array[Byte]],
    11 -> lPushCoder[String],
    12 -> lPushCoder[Array[Byte]],
    13 -> rPushCoder[String],
    14 -> rPushCoder[Array[Byte]],
    15 -> pfAddCoder[String],
    16 -> pfAddCoder[Array[Byte]]
  )

  implicit def redisMutationCoder[T <: RedisMutation]: Coder[T] =
    Coder.disjunction[T, Int]("RedisMutation", coders.asInstanceOf[Map[Int, Coder[T]]]) {
      case RedisMutation(_: Append[String @unchecked], RedisType.StringRedisType)         => 1
      case RedisMutation(_: Append[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) => 2
      case RedisMutation(_: Set[String @unchecked], RedisType.StringRedisType)            => 3
      case RedisMutation(_: Set[Array[Byte] @unchecked], RedisType.ByteArrayRedisType)    => 4
      case RedisMutation(_: IncrBy[String @unchecked], RedisType.StringRedisType)         => 5
      case RedisMutation(_: IncrBy[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) => 6
      case RedisMutation(_: DecrBy[String @unchecked], RedisType.StringRedisType)         => 7
      case RedisMutation(_: DecrBy[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) => 8
      case RedisMutation(_: SAdd[String @unchecked], RedisType.StringRedisType)           => 9
      case RedisMutation(_: SAdd[Array[Byte] @unchecked], RedisType.ByteArrayRedisType)   => 10
      case RedisMutation(_: LPush[String @unchecked], RedisType.StringRedisType)          => 11
      case RedisMutation(_: LPush[Array[Byte] @unchecked], RedisType.ByteArrayRedisType)  => 12
      case RedisMutation(_: RPush[String @unchecked], RedisType.StringRedisType)          => 13
      case RedisMutation(_: RPush[Array[Byte] @unchecked], RedisType.ByteArrayRedisType)  => 14
      case RedisMutation(_: PFAdd[String @unchecked], RedisType.StringRedisType)          => 15
      case RedisMutation(_: PFAdd[Array[Byte] @unchecked], RedisType.ByteArrayRedisType)  => 16
    }

}
