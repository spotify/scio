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

import com.spotify.scio.redis.types._
import redis.clients.jedis.{Pipeline, Response}

import java.lang.{Long => JLong}

sealed abstract class RedisMutator[T] extends Serializable {
  def mutate(client: Pipeline, mutation: T): List[Response[_]]
}

object RedisMutator {

  implicit val stringAppend: RedisMutator[Append[String]] =
    new RedisMutator[Append[String]] {
      override def mutate(client: Pipeline, mutation: Append[String]): List[Response[JLong]] = {
        client.append(mutation.key, mutation.value) ::
          mutation.ttl
            .map(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
            .toList
      }
    }

  implicit val byteArrayAppend: RedisMutator[Append[Array[Byte]]] =
    new RedisMutator[Append[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: Append[Array[Byte]]): List[Response[JLong]] =
        client.append(mutation.key, mutation.value) ::
          mutation.ttl
            .map(expireTime => client.pexpire(mutation.key, expireTime.getMillis))
            .toList
    }

  implicit val stringSet: RedisMutator[Set[String]] =
    new RedisMutator[Set[String]] {
      override def mutate(client: Pipeline, mutation: Set[String]): List[Response[_]] =
        mutation.ttl.fold(client.set(mutation.key, mutation.value)) { ttl =>
          client.psetex(mutation.key, ttl.getMillis(), mutation.value)
        } :: Nil
    }

  implicit val byteArraySet: RedisMutator[Set[Array[Byte]]] =
    new RedisMutator[Set[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: Set[Array[Byte]]): List[Response[_]] =
        mutation.ttl.fold(client.set(mutation.key, mutation.value)) { ttl =>
          client.psetex(mutation.key, ttl.getMillis(), mutation.value)
        } :: Nil
    }

  implicit val stringIncrBy: RedisMutator[IncrBy[String]] =
    new RedisMutator[IncrBy[String]] {
      override def mutate(client: Pipeline, mutation: IncrBy[String]): List[Response[JLong]] =
        client.incrBy(mutation.key, mutation.value) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val byteArrayIncrBy: RedisMutator[IncrBy[Array[Byte]]] =
    new RedisMutator[IncrBy[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: IncrBy[Array[Byte]]): List[Response[JLong]] =
        client.incrBy(mutation.key, mutation.value) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val stringDecrBy: RedisMutator[DecrBy[String]] =
    new RedisMutator[DecrBy[String]] {
      override def mutate(client: Pipeline, mutation: DecrBy[String]): List[Response[JLong]] =
        client.decrBy(mutation.key, mutation.value) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val byteArrayDecrBy: RedisMutator[DecrBy[Array[Byte]]] =
    new RedisMutator[DecrBy[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: DecrBy[Array[Byte]]): List[Response[JLong]] =
        client.decrBy(mutation.key, mutation.value) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val stringSAdd: RedisMutator[SAdd[String]] =
    new RedisMutator[SAdd[String]] {
      override def mutate(client: Pipeline, mutation: SAdd[String]): List[Response[JLong]] =
        client.sadd(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val byteArraySAdd: RedisMutator[SAdd[Array[Byte]]] =
    new RedisMutator[SAdd[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: SAdd[Array[Byte]]): List[Response[JLong]] =
        client.sadd(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val stringLPush: RedisMutator[LPush[String]] =
    new RedisMutator[LPush[String]] {
      override def mutate(client: Pipeline, mutation: LPush[String]): List[Response[JLong]] =
        client.lpush(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val byteArrayLPush: RedisMutator[LPush[Array[Byte]]] =
    new RedisMutator[LPush[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: LPush[Array[Byte]]): List[Response[JLong]] =
        client.lpush(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val stringRPush: RedisMutator[RPush[String]] =
    new RedisMutator[RPush[String]] {
      override def mutate(client: Pipeline, mutation: RPush[String]): List[Response[JLong]] =
        client.rpush(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val byteArrayRPush: RedisMutator[RPush[Array[Byte]]] =
    new RedisMutator[RPush[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: RPush[Array[Byte]]): List[Response[JLong]] =
        client.rpush(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val stringPFAdd: RedisMutator[PFAdd[String]] =
    new RedisMutator[PFAdd[String]] {
      override def mutate(client: Pipeline, mutation: PFAdd[String]): List[Response[JLong]] =
        client.pfadd(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit val byteArrayPFAdd: RedisMutator[PFAdd[Array[Byte]]] =
    new RedisMutator[PFAdd[Array[Byte]]] {
      override def mutate(client: Pipeline, mutation: PFAdd[Array[Byte]]): List[Response[JLong]] =
        client.pfadd(mutation.key, mutation.value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(mutation.key, expireTime.getMillis)).toList
    }

  implicit def redisMutator[T <: RedisMutation]: RedisMutator[T] =
    new RedisMutator[T] {
      override def mutate(client: Pipeline, mutation: T): List[Response[_]] = {
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

  def mutate[T <: RedisMutation: RedisMutator](client: Pipeline)(value: T): List[Response[_]] =
    implicitly[RedisMutator[T]].mutate(client, value)

}
