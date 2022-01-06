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

package com.spotify.scio.redis.types

import org.joda.time.Duration

/**
 * Represents an abstract Redis command. See Redis commands documentation for the description of
 * commands: https://redis.io/commands
 */
sealed abstract class RedisMutation extends Serializable {
  def rt: RedisType[_]
}

object RedisMutation {
  def unapply[T <: RedisMutation](mt: T): Option[(T, RedisType[_])] = Some(mt -> mt.rt)
}

final case class Append[T](key: T, value: T, ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class Set[T](key: T, value: T, ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class IncrBy[T](key: T, value: Long, ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class DecrBy[T](key: T, value: Long, ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class SAdd[T](key: T, value: Seq[T], ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class LPush[T](key: T, value: Seq[T], ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class RPush[T](key: T, value: Seq[T], ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class PFAdd[T](key: T, value: Seq[T], ttl: Option[Duration] = None)(implicit
  val rt: RedisType[T]
) extends RedisMutation
final case class ZAdd[T](key: T, scoreMembers: Map[T, Double], ttl: Option[Duration] = None)(
  implicit val rt: RedisType[T]
) extends RedisMutation
object ZAdd {
  final def apply[T: RedisType](key: T, score: Double, member: T): ZAdd[T] =
    ZAdd(key, Map(member -> score))

  def apply[T: RedisType](key: T, score: Double, member: T, ttl: Option[Duration]): ZAdd[T] =
    ZAdd(key, Map(member -> score), ttl)
}
