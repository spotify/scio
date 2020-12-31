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
import com.spotify.scio.redis.types.RedisType.ByteArrayRedisType
import com.spotify.scio.redis.types.RedisType.StringRedisType
import redis.clients.jedis.util.SafeEncoder

sealed abstract class RedisMutator[T] extends Serializable {
  def mutate(client: Pipeline, mutation: T): List[Response[_]]
}

object RedisMutator {
  implicit def appendMutator[T]: RedisMutator[Append[T]] = new RedisMutator[Append[T]] {
    override def mutate(client: Pipeline, mutation: Append[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), SafeEncoder.encode(mutation.value))
      }

      client.append(key, value) ::
        mutation.ttl
          .map(expireTime => client.pexpire(key, expireTime.getMillis))
          .toList
    }
  }

  implicit def setMutator[T]: RedisMutator[Set[T]] = new RedisMutator[Set[T]] {
    override def mutate(client: Pipeline, mutation: Set[T]): List[Response[_]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), SafeEncoder.encode(mutation.value))
      }

      mutation.ttl.fold(client.set(key, value)) { ttl =>
        client.psetex(key, ttl.getMillis(), value)
      } :: Nil
    }
  }

  implicit def incrByMutator[T]: RedisMutator[IncrBy[T]] = new RedisMutator[IncrBy[T]] {
    override def mutate(client: Pipeline, mutation: IncrBy[T]): List[Response[JLong]] = {
      val key = mutation.rt match {
        case ByteArrayRedisType => mutation.key
        case StringRedisType    => SafeEncoder.encode(mutation.key)
      }
      client.incrBy(key, mutation.value) ::
        mutation.ttl.map(expireTime => client.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def decrByMutator[T]: RedisMutator[DecrBy[T]] = new RedisMutator[DecrBy[T]] {
    override def mutate(client: Pipeline, mutation: DecrBy[T]): List[Response[JLong]] = {
      val key = mutation.rt match {
        case ByteArrayRedisType => mutation.key
        case StringRedisType    => SafeEncoder.encode(mutation.key)
      }
      client.decrBy(key, mutation.value) ::
        mutation.ttl.map(expireTime => client.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def saddMutator[T]: RedisMutator[SAdd[T]] = new RedisMutator[SAdd[T]] {
    override def mutate(client: Pipeline, mutation: SAdd[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
      }

      client.sadd(key, value: _*) ::
        mutation.ttl.map(expireTime => client.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def lpushMutator[T]: RedisMutator[LPush[T]] = new RedisMutator[LPush[T]] {
    override def mutate(client: Pipeline, mutation: LPush[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
      }

      client.lpush(key, value: _*) ::
        mutation.ttl.map(expireTime => client.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def rpushMutator[T]: RedisMutator[RPush[T]] = new RedisMutator[RPush[T]] {
    override def mutate(client: Pipeline, mutation: RPush[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
      }

      client.rpush(key, value: _*) ::
        mutation.ttl.map(expireTime => client.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def pfaddMutator[T]: RedisMutator[PFAdd[T]] =
    new RedisMutator[PFAdd[T]] {
      override def mutate(client: Pipeline, mutation: PFAdd[T]): List[Response[JLong]] = {
        val (key, value) = mutation.rt match {
          case ByteArrayRedisType =>
            (mutation.key, mutation.value)
          case StringRedisType =>
            (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
        }

        client.pfadd(key, value: _*) ::
          mutation.ttl.map(expireTime => client.pexpire(key, expireTime.getMillis)).toList
      }
    }

  implicit def redisMutator[T <: RedisMutation]: RedisMutator[T] = new RedisMutator[T] {
    override def mutate(client: Pipeline, mutation: T): List[Response[_]] = {
      mutation match {
        case mt @ Append(_, _, _) => RedisMutator.mutate(client)(mt)
        case mt @ Set(_, _, _)    => RedisMutator.mutate(client)(mt)
        case mt @ IncrBy(_, _, _) => RedisMutator.mutate(client)(mt)
        case mt @ DecrBy(_, _, _) => RedisMutator.mutate(client)(mt)
        case mt @ SAdd(_, _, _)   => RedisMutator.mutate(client)(mt)
        case mt @ LPush(_, _, _)  => RedisMutator.mutate(client)(mt)
        case mt @ RPush(_, _, _)  => RedisMutator.mutate(client)(mt)
        case mt @ PFAdd(_, _, _)  => RedisMutator.mutate(client)(mt)
      }
    }
  }

  def mutate[T <: RedisMutation: RedisMutator](client: Pipeline)(value: T): List[Response[_]] =
    implicitly[RedisMutator[T]].mutate(client, value)

}
