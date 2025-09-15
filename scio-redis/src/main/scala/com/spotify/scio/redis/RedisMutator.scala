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
import redis.clients.jedis.{Response, Transaction}

import java.lang.{Double => jDouble, Long => JLong}
import com.spotify.scio.redis.types.RedisType.{ByteArrayRedisType, StringRedisType}
import redis.clients.jedis.util.SafeEncoder

import scala.jdk.CollectionConverters._

sealed abstract class RedisMutator[T] extends Serializable {
  def mutate(transaction: Transaction, mutation: T): List[Response[_]]
}

object RedisMutator {
  implicit def appendMutator[T]: RedisMutator[Append[T]] = new RedisMutator[Append[T]] {
    override def mutate(transaction: Transaction, mutation: Append[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), SafeEncoder.encode(mutation.value))
      }

      transaction.append(key, value) ::
        mutation.ttl
          .map(expireTime => transaction.pexpire(key, expireTime.getMillis))
          .toList
    }
  }

  implicit def setMutator[T]: RedisMutator[Set[T]] = new RedisMutator[Set[T]] {
    override def mutate(transaction: Transaction, mutation: Set[T]): List[Response[_]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), SafeEncoder.encode(mutation.value))
      }

      mutation.ttl.fold(transaction.set(key, value)) { ttl =>
        transaction.psetex(key, ttl.getMillis(), value)
      } :: Nil
    }
  }

  implicit def incrByMutator[T]: RedisMutator[IncrBy[T]] = new RedisMutator[IncrBy[T]] {
    override def mutate(transaction: Transaction, mutation: IncrBy[T]): List[Response[JLong]] = {
      val key = mutation.rt match {
        case ByteArrayRedisType => mutation.key
        case StringRedisType    => SafeEncoder.encode(mutation.key)
      }
      transaction.incrBy(key, mutation.value) ::
        mutation.ttl.map(expireTime => transaction.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def decrByMutator[T]: RedisMutator[DecrBy[T]] = new RedisMutator[DecrBy[T]] {
    override def mutate(transaction: Transaction, mutation: DecrBy[T]): List[Response[JLong]] = {
      val key = mutation.rt match {
        case ByteArrayRedisType => mutation.key
        case StringRedisType    => SafeEncoder.encode(mutation.key)
      }
      transaction.decrBy(key, mutation.value) ::
        mutation.ttl.map(expireTime => transaction.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def saddMutator[T]: RedisMutator[SAdd[T]] = new RedisMutator[SAdd[T]] {
    override def mutate(transaction: Transaction, mutation: SAdd[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
      }

      transaction.sadd(key, value: _*) ::
        mutation.ttl.map(expireTime => transaction.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def lpushMutator[T]: RedisMutator[LPush[T]] = new RedisMutator[LPush[T]] {
    override def mutate(transaction: Transaction, mutation: LPush[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
      }

      transaction.lpush(key, value: _*) ::
        mutation.ttl.map(expireTime => transaction.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def rpushMutator[T]: RedisMutator[RPush[T]] = new RedisMutator[RPush[T]] {
    override def mutate(transaction: Transaction, mutation: RPush[T]): List[Response[JLong]] = {
      val (key, value) = mutation.rt match {
        case ByteArrayRedisType =>
          (mutation.key, mutation.value)
        case StringRedisType =>
          (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
      }

      transaction.rpush(key, value: _*) ::
        mutation.ttl.map(expireTime => transaction.pexpire(key, expireTime.getMillis)).toList
    }
  }

  implicit def pfaddMutator[T]: RedisMutator[PFAdd[T]] =
    new RedisMutator[PFAdd[T]] {
      override def mutate(transaction: Transaction, mutation: PFAdd[T]): List[Response[JLong]] = {
        val (key, value) = mutation.rt match {
          case ByteArrayRedisType =>
            (mutation.key, mutation.value)
          case StringRedisType =>
            (SafeEncoder.encode(mutation.key), mutation.value.map(SafeEncoder.encode(_)))
        }

        transaction.pfadd(key, value: _*) ::
          mutation.ttl.map(expireTime => transaction.pexpire(key, expireTime.getMillis)).toList
      }
    }

  implicit def zaddMutator[T]: RedisMutator[ZAdd[T]] =
    new RedisMutator[ZAdd[T]] {
      override def mutate(transaction: Transaction, mutation: ZAdd[T]): List[Response[JLong]] = {
        val (key, scoreMembers) = mutation.rt match {
          case ByteArrayRedisType =>
            (
              mutation.key,
              mutation.scoreMembers.map { case (k, v) =>
                k -> jDouble.valueOf(v)
              }
            )
          case StringRedisType =>
            (
              SafeEncoder.encode(mutation.key),
              mutation.scoreMembers.map { case (k, v) =>
                SafeEncoder.encode(k) -> jDouble.valueOf(v)
              }
            )
        }

        transaction.zadd(key, scoreMembers.asJava) ::
          mutation.ttl.map(expireTime => transaction.pexpire(key, expireTime.getMillis)).toList
      }
    }

  implicit def redisMutator[T <: RedisMutation]: RedisMutator[T] = new RedisMutator[T] {
    override def mutate(transaction: Transaction, mutation: T): List[Response[_]] = {
      mutation match {
        case mt @ Append(_, _, _) => appendMutator.mutate(transaction, mt)
        case mt @ Set(_, _, _)    => setMutator.mutate(transaction, mt)
        case mt @ IncrBy(_, _, _) => incrByMutator.mutate(transaction, mt)
        case mt @ DecrBy(_, _, _) => decrByMutator.mutate(transaction, mt)
        case mt @ SAdd(_, _, _)   => saddMutator.mutate(transaction, mt)
        case mt @ LPush(_, _, _)  => lpushMutator.mutate(transaction, mt)
        case mt @ RPush(_, _, _)  => rpushMutator.mutate(transaction, mt)
        case mt @ PFAdd(_, _, _)  => pfaddMutator.mutate(transaction, mt)
        case mt @ ZAdd(_, _, _)   => zaddMutator.mutate(transaction, mt)
      }
    }
  }

  def mutate[T <: RedisMutation: RedisMutator](transaction: Transaction)(
    value: T
  ): List[Response[_]] =
    implicitly[RedisMutator[T]].mutate(transaction, value)

}
