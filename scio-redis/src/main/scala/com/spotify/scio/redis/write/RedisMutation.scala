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

import redis.clients.jedis.Pipeline

/** Represents an abstract Redis command. */
sealed trait RedisMutation extends Product with Serializable

/** See Redis commands documentation for the description of commands: https://redis.io/commands */
object RedisMutation {

  // Subspace of mutations having string key
  object String {

    // String mutations
    case class Append(key: String, value: String, ttl: Option[Long] = None) extends RedisMutation

    case class Set(key: String, value: String, ttl: Option[Long] = None) extends RedisMutation

    case class IncrBy(key: String, value: Long, ttl: Option[Long] = None) extends RedisMutation

    case class DecrBy(key: String, value: Long, ttl: Option[Long] = None) extends RedisMutation

    // Set mutations
    case class SAdd(key: String, value: Seq[String], ttl: Option[Long] = None) extends RedisMutation

    // List mutations
    case class RPush(key: String, value: Seq[String], ttl: Option[Long] = None)
        extends RedisMutation

    case class LPush(key: String, value: Seq[String], ttl: Option[Long] = None)
        extends RedisMutation

    // HyperLogLog mutations
    case class PFAdd(key: String, value: Seq[String], ttl: Option[Long] = None)
        extends RedisMutation

  }

  // Subspace of mutations having byte-array key
  object ByteArray {

    // String mutations
    case class Append(key: Array[Byte], value: Array[Byte], ttl: Option[Long] = None)
        extends RedisMutation

    case class Set(key: Array[Byte], value: Array[Byte], ttl: Option[Long] = None)
        extends RedisMutation

    case class IncrBy(key: Array[Byte], value: Long, ttl: Option[Long] = None) extends RedisMutation

    case class DecrBy(key: Array[Byte], value: Long, ttl: Option[Long] = None) extends RedisMutation

    // Set mutations
    case class SAdd(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Long] = None)
        extends RedisMutation

    // List mutations
    case class RPush(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Long] = None)
        extends RedisMutation

    case class LPush(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Long] = None)
        extends RedisMutation

    // HyperLogLog mutations
    case class PFAdd(key: Array[Byte], value: Seq[Array[Byte]], ttl: Option[Long] = None)
        extends RedisMutation

  }

}

object RedisMutator {

  import RedisMutation._

  private def setTtl(pipeline: Pipeline, key: String, ttl: Option[Long]): Unit = ttl match {
    case Some(expireTime) => pipeline.pexpire(key, expireTime)
    case None             => ()
  }

  private def setTtl(pipeline: Pipeline, key: Array[Byte], ttl: Option[Long]): Unit = ttl match {
    case Some(expireTime) => pipeline.pexpire(key, expireTime)
    case None             => ()
  }

  /**
   * Applies a mutation to the Redis pipeline.
   * Sets ttl if needed.
   */
  def apply(mutation: RedisMutation, pipeline: Pipeline): Unit = {
    mutation match {
      // Append
      case String.Append(key, value, ttl) =>
        pipeline.append(key, value)
        setTtl(pipeline, key, ttl)
      case ByteArray.Append(key, value, _) => pipeline.append(key, value)
      // Set
      case String.Set(key, value, ttl) =>
        ttl match {
          case Some(expireTime) => pipeline.psetex(key, expireTime, value)
          case None             => pipeline.set(key, value)
        }
      case ByteArray.Set(key, value, ttl) =>
        ttl match {
          case Some(expireTime) => pipeline.psetex(key, expireTime, value)
          case None             => pipeline.set(key, value)
        }
      // IncrBy
      case String.IncrBy(key, value, ttl) =>
        pipeline.incrBy(key, value)
        setTtl(pipeline, key, ttl)
      case ByteArray.IncrBy(key, value, ttl) =>
        pipeline.incrBy(key, value)
        setTtl(pipeline, key, ttl)
      // DecrBy
      case String.DecrBy(key, value, ttl) =>
        pipeline.decrBy(key, value)
        setTtl(pipeline, key, ttl)
      case ByteArray.DecrBy(key, value, ttl) =>
        pipeline.decrBy(key, value)
        setTtl(pipeline, key, ttl)
      // LPush
      case String.LPush(key, values, ttl) =>
        pipeline.lpush(key, values: _*)
        setTtl(pipeline, key, ttl)
      case ByteArray.LPush(key, values, ttl) =>
        pipeline.lpush(key, values: _*)
        setTtl(pipeline, key, ttl)
      // RPush
      case String.RPush(key, values, ttl) =>
        pipeline.rpush(key, values: _*)
        setTtl(pipeline, key, ttl)
      case ByteArray.RPush(key, values, ttl) =>
        pipeline.rpush(key, values: _*)
        setTtl(pipeline, key, ttl)
      // SAdd
      case String.SAdd(key, value, ttl) =>
        pipeline.sadd(key, value: _*)
        setTtl(pipeline, key, ttl)
      case ByteArray.SAdd(key, value, ttl) =>
        pipeline.sadd(key, value: _*)
        setTtl(pipeline, key, ttl)
      // PFAdd
      case String.PFAdd(key, value, ttl) =>
        pipeline.pfadd(key, value: _*)
        setTtl(pipeline, key, ttl)
      case ByteArray.PFAdd(key, value, ttl) =>
        pipeline.pfadd(key, value: _*)
        setTtl(pipeline, key, ttl)
    }
  }
}
