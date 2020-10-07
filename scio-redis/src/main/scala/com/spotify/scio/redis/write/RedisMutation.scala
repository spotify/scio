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

package com.spotify.scio.redis.write

import org.joda.time.Duration
import redis.clients.jedis.Pipeline

/** Represents an abstract Redis command. */
sealed trait RedisMutation[T] extends Serializable {
  val key: T
  val ttl: Option[Duration]
}

// This is needed to overcome the issue with type erasure
sealed trait StringKeyMutation extends RedisMutation[String]

// This is needed to overcome the issue with type erasure
sealed trait ByteArrayKeyMutation extends RedisMutation[Array[Byte]]

/** See Redis commands documentation for the description of commands: https://redis.io/commands */
object RedisMutation {

  // Subspace of mutations having string key
  object String {

    // String mutations
    case class Append(key: String, value: String, ttl: Option[Duration] = None)
        extends StringKeyMutation

    case class Set(key: String, value: String, ttl: Option[Duration] = None)
        extends StringKeyMutation

    case class IncrBy(key: String, value: Long, ttl: Option[Duration] = None)
        extends StringKeyMutation

    case class DecrBy(key: String, value: Long, ttl: Option[Duration] = None)
        extends StringKeyMutation

    // Set mutations
    case class SAdd(key: String, value: Seq[String], ttl: Option[Duration] = None)
        extends StringKeyMutation

    // List mutations
    case class RPush(key: String, value: Seq[String], ttl: Option[Duration] = None)
        extends StringKeyMutation

    case class LPush(key: String, value: Seq[String], ttl: Option[Duration] = None)
        extends StringKeyMutation

    // HyperLogLog mutations
    case class PFAdd(key: String, value: Seq[String], ttl: Option[Duration] = None)
        extends StringKeyMutation

  }

  // Subspace of mutations having byte-array key
  object ByteArray {

    // String mutations
    case class Append(key: Array[Byte], value: Array[Byte], ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

    case class Set(key: Array[Byte], value: Array[Byte], ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

    case class IncrBy(key: Array[Byte], value: Long, ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

    case class DecrBy(key: Array[Byte], value: Long, ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

    // Set mutations
    case class SAdd(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

    // List mutations
    case class RPush(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

    case class LPush(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

    // HyperLogLog mutations
    case class PFAdd(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Duration] = None)
        extends ByteArrayKeyMutation

  }

}

object RedisMutator {

  import RedisMutation._

  /**
   * Applies a mutation to the Redis pipeline.
   * Sets ttl if needed.
   */
  def apply(mutation: RedisMutation[_], pipeline: Pipeline): Unit = {
    mutation match {
      // Append
      case String.Append(key, value, _)    => pipeline.append(key, value)
      case ByteArray.Append(key, value, _) => pipeline.append(key, value)
      // Set
      case String.Set(key, value, _)    => pipeline.set(key, value)
      case ByteArray.Set(key, value, _) => pipeline.set(key, value)
      // IncrBy
      case String.IncrBy(key, value, _)    => pipeline.incrBy(key, value)
      case ByteArray.IncrBy(key, value, _) => pipeline.incrBy(key, value)
      // DecrBy
      case String.DecrBy(key, value, _)    => pipeline.decrBy(key, value)
      case ByteArray.DecrBy(key, value, _) => pipeline.decrBy(key, value)
      // LPush
      case String.LPush(key, values, _)    => pipeline.lpush(key, values: _*)
      case ByteArray.LPush(key, values, _) => pipeline.lpush(key, values: _*)
      // RPush
      case String.RPush(key, values, _)    => pipeline.rpush(key, values: _*)
      case ByteArray.RPush(key, values, _) => pipeline.rpush(key, values: _*)
      // SAdd
      case String.SAdd(key, value, _)    => pipeline.sadd(key, value: _*)
      case ByteArray.SAdd(key, value, _) => pipeline.sadd(key, value: _*)
      // PFAdd
      case String.PFAdd(key, value, _)    => pipeline.pfadd(key, value: _*)
      case ByteArray.PFAdd(key, value, _) => pipeline.pfadd(key, value: _*)
    }

    mutation match {
      case mutation: StringKeyMutation =>
        mutation.ttl.foreach(expireTime => pipeline.pexpire(mutation.key, expireTime.getMillis))
      case mutation: ByteArrayKeyMutation =>
        mutation.ttl.foreach(expireTime => pipeline.pexpire(mutation.key, expireTime.getMillis))
    }

  }

}
