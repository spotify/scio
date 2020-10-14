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

/**
 * Represents an abstract Redis command.
 * See Redis commands documentation for the description of commands: https://redis.io/commands
 */
sealed abstract class RedisMutation extends Product with Serializable {
  def rt: RedisType[_]
}

object RedisMutation {
  def unapply[T <: RedisMutation](mt: T): Option[(T, RedisType[_])] = Some(mt -> mt.rt)
}

sealed abstract class RedisType[T]

object RedisType {
  implicit case object StringRedisType extends RedisType[String]
  implicit case object ByteArrayRedisType extends RedisType[Array[Byte]]
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

sealed abstract class RedisMutator[-T] extends Serializable {
  def mutate(client: Pipeline, mutation: T): Unit
}

object RedisMutator {

  implicit val stringAppend: RedisMutator[Append[String]] =
    new RedisMutator[Append[String]] {
      override def mutate(client: Pipeline, mutation: Append[String]): Unit = {
        client.append(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArrayAppend: RedisMutator[Append[Array[Byte]]] =
    new RedisMutator[Append[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: Append[Array[Byte]]): Unit = {
        client.append(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val stringSet: RedisMutator[Set[String]] =
    new RedisMutator[Set[String]] {
      override def mutate(client: Pipeline, mutation: Set[String]): Unit = {
        client.set(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArraySet: RedisMutator[Set[Array[Byte]]] =
    new RedisMutator[Set[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: Set[Array[Byte]]): Unit = {
        client.set(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val stringIncrBy: RedisMutator[IncrBy[String]] =
    new RedisMutator[IncrBy[String]] {
      override def mutate(client: Pipeline, mutation: IncrBy[String]): Unit = {
        client.incrBy(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArrayIncrBy: RedisMutator[IncrBy[Array[Byte]]] =
    new RedisMutator[IncrBy[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: IncrBy[Array[Byte]]): Unit = {
        client.incrBy(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val stringDecrBy: RedisMutator[DecrBy[String]] =
    new RedisMutator[DecrBy[String]] {
      override def mutate(client: Pipeline, mutation: DecrBy[String]): Unit = {
        client.decrBy(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArrayDecrBy: RedisMutator[DecrBy[Array[Byte]]] =
    new RedisMutator[DecrBy[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: DecrBy[Array[Byte]]): Unit = {
        client.decrBy(mutation.key, mutation.value)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val stringSAdd: RedisMutator[SAdd[String]] =
    new RedisMutator[SAdd[String]] {
      override def mutate(client: Pipeline, mutation: SAdd[String]): Unit = {
        client.sadd(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArraySAdd: RedisMutator[SAdd[Array[Byte]]] =
    new RedisMutator[SAdd[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: SAdd[Array[Byte]]): Unit = {
        client.sadd(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val stringLPush: RedisMutator[LPush[String]] =
    new RedisMutator[LPush[String]] {
      override def mutate(client: Pipeline, mutation: LPush[String]): Unit = {
        client.lpush(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArrayLPush: RedisMutator[LPush[Array[Byte]]] =
    new RedisMutator[LPush[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: LPush[Array[Byte]]): Unit = {
        client.lpush(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val stringRPush: RedisMutator[RPush[String]] =
    new RedisMutator[RPush[String]] {
      override def mutate(client: Pipeline, mutation: RPush[String]): Unit = {
        client.rpush(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArrayRPush: RedisMutator[RPush[Array[Byte]]] =
    new RedisMutator[RPush[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: RPush[Array[Byte]]): Unit = {
        client.rpush(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val stringPFAdd: RedisMutator[PFAdd[String]] =
    new RedisMutator[PFAdd[String]] {
      override def mutate(client: Pipeline, mutation: PFAdd[String]): Unit = {
        client.pfadd(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit val byteArrayPFAdd: RedisMutator[PFAdd[Array[Byte]]] =
    new RedisMutator[PFAdd[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: PFAdd[Array[Byte]]): Unit = {
        client.pfadd(mutation.key, mutation.value: _*)
        mutation.ttl.foreach(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
      }
    }

  implicit def redisMutator[T <: RedisMutation]: RedisMutator[T] =
    new RedisMutator[T] {
      override def mutate(client: Pipeline, mutation: T): Unit = {
        mutation match {
          case RedisMutation(mt: Append[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: Append[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: Set[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: Set[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: IncrBy[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: IncrBy[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: DecrBy[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: DecrBy[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: SAdd[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: SAdd[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: RPush[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: RPush[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: LPush[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: LPush[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: PFAdd[Array[Byte] @unchecked], RedisType.ByteArrayRedisType) =>
            RedisMutator.mutate(client)(mt)
          case RedisMutation(mt: PFAdd[String @unchecked], RedisType.StringRedisType) =>
            RedisMutator.mutate(client)(mt)
        }
      }
    }

  def mutate[T <: RedisMutation: RedisMutator](client: Pipeline)(value: T): Unit =
    implicitly[RedisMutator[T]].mutate(client, value)

}
