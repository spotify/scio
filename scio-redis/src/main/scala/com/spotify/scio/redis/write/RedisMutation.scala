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

/** Represents an abstract Redis command.
 * See Redis commands documentation for the description of commands: https://redis.io/commands
 * */
sealed trait RedisMutation[T] extends Product with Serializable {
  val key: T
  val ttl: Option[Duration]
}

sealed abstract class RedisType[T]

object RedisType {
  implicit case object StringRedisType extends RedisType[String]
  implicit case object ByteArrayRedisType extends RedisType[Array[Byte]]
}

final case class Append[T: RedisType](key: T, value: T, ttl: Option[Duration] = None)
  extends RedisMutation[T]
final case class Set[T: RedisType](key: T, value: T, ttl: Option[Duration] = None)
  extends RedisMutation[T]
final case class IncrBy[T: RedisType](key: T, value: Long, ttl: Option[Duration] = None)
  extends RedisMutation[T]
final case class DecrBy[T: RedisType](key: T, value: Long, ttl: Option[Duration] = None)
  extends RedisMutation[T]
final case class SAdd[T: RedisType](key: T, value: Seq[T], ttl: Option[Duration] = None)
  extends RedisMutation[T]
final case class LPush[T: RedisType](key: T, value: Seq[T], ttl: Option[Duration] = None)
  extends RedisMutation[T]
final case class RPush[T: RedisType](key: T, value: Seq[T], ttl: Option[Duration] = None)
  extends RedisMutation[T]
final case class PFAdd[T: RedisType](key: T, value: Seq[T], ttl: Option[Duration] = None)
  extends RedisMutation[T]

sealed abstract class RedisMutator[-T] extends Serializable {
  def mutate(client: Pipeline, mutation: T): Unit
}

object RedisMutator {

  implicit val stringRedisMutator: RedisMutator[RedisMutation[String]] =
    new RedisMutator[RedisMutation[String]] {
      override def mutate(client: Pipeline, mutation: RedisMutation[String]): Unit = {
        mutation match {
          case Append(key, value, _) => client.append(key, value)
          case Set(key, value, _) => client.set(key, value)
          case IncrBy(key, value, _) => client.incrBy(key, value)
          case DecrBy(key, value, _) => client.decrBy(key, value)
          case SAdd(key, value, _) => client.sadd(key, value: _*)
          case LPush(key, value, _) => client.lpush(key, value: _*)
          case RPush(key, value, _) => client.rpush(key, value: _*)
          case PFAdd(key, value, _) => client.pfadd(key, value: _*)
        }

        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArrayRedisMutator: RedisMutator[RedisMutation[Array[Byte]]] =
    new RedisMutator[RedisMutation[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: RedisMutation[Array[Byte]]): Unit = {
        mutation match {
          case Append(key, value, _) => client.append(key, value)
          case Set(key, value, _) => client.set(key, value)
          case IncrBy(key, value, _) => client.incrBy(key, value)
          case DecrBy(key, value, _) => client.decrBy(key, value)
          case SAdd(key, value, _) => client.sadd(key, value: _*)
          case LPush(key, value, _) => client.lpush(key, value: _*)
          case RPush(key, value, _) => client.rpush(key, value: _*)
          case PFAdd(key, value, _) => client.pfadd(key, value: _*)
        }

        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  def mutate[T: RedisMutator](client: Pipeline)(value: T): Unit =
    implicitly[RedisMutator[T]].mutate(client, value)

}
