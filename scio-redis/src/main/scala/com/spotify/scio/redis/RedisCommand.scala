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

package com.spotify.scio.redis

import org.apache.beam.sdk.io.redis.{RedisIO => BeamRedisIO}

/** Represents an abstract Redis command. */
sealed trait RedisCommand extends Serializable

/**
 * A Redis command which mutates a Redis data structure.
 * @tparam K Type of a key. Could be string or byte array.
 * @tparam V Type of a value being transferred to Redis as a part of the mutation.
 *           This might be a byte array, string, etc.
 */
final case class MutationRedisCommand[K, V](command: BeamRedisIO.Write.Method, key: K, value: V)
    extends RedisCommand

sealed trait RedisValueTypeEvidence[T]

object RedisValueTypeEvidence {
  implicit val stringValueType: RedisValueTypeEvidence[String] =
    new RedisValueTypeEvidence[String] {}

  implicit val byteArrayValueType: RedisValueTypeEvidence[Array[Byte]] =
    new RedisValueTypeEvidence[Array[Byte]] {}
}

/**
 * TODO: this isn't been used yet, need to change Beam RedisIO to support both binary and string
 * keys and values.
 */
object RedisCommand {

  object String {

    def append[K: RedisValueTypeEvidence, V: RedisValueTypeEvidence](key: K, value: V)(implicit
      eq: K =:= V
    ): MutationRedisCommand[K, V] =
      MutationRedisCommand(BeamRedisIO.Write.Method.APPEND, key, value)

    def set[K: RedisValueTypeEvidence, V: RedisValueTypeEvidence](key: K, value: V)(implicit
      eq: K =:= V
    ): MutationRedisCommand[K, V] =
      MutationRedisCommand(BeamRedisIO.Write.Method.SET, key, value)

    def incrBy[K: RedisValueTypeEvidence](key: K, value: Long): MutationRedisCommand[K, Long] =
      MutationRedisCommand(BeamRedisIO.Write.Method.INCRBY, key, value)

    def decrBy[K: RedisValueTypeEvidence](key: K, value: Long): MutationRedisCommand[K, Long] =
      MutationRedisCommand(BeamRedisIO.Write.Method.INCRBY, key, value)

  }

  object List {

    def rPush[K: RedisValueTypeEvidence, V: RedisValueTypeEvidence](key: K, value: V)(implicit
      eq: K =:= V
    ): MutationRedisCommand[K, V] =
      MutationRedisCommand(BeamRedisIO.Write.Method.RPUSH, key, value)

    def lPush[K: RedisValueTypeEvidence, V: RedisValueTypeEvidence](key: K, value: V)(implicit
      eq: K =:= V
    ): MutationRedisCommand[K, V] =
      MutationRedisCommand(BeamRedisIO.Write.Method.LPUSH, key, value)

  }

  object Set {

    def sAdd[K: RedisValueTypeEvidence, V: RedisValueTypeEvidence](key: K, value: V)(implicit
      eq: K =:= V
    ): MutationRedisCommand[K, V] =
      MutationRedisCommand(BeamRedisIO.Write.Method.SADD, key, value)

  }

  object HyperLogLog {

    def pFAdd[K: RedisValueTypeEvidence, V: RedisValueTypeEvidence](key: K, value: V)(implicit
      eq: K =:= V
    ): MutationRedisCommand[K, V] =
      MutationRedisCommand(BeamRedisIO.Write.Method.PFADD, key, value)

  }

}
