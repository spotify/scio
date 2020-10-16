package com.spotify.scio.redis.instances

import com.spotify.scio.coders.Coder
import com.spotify.scio.redis.write._

trait CoderInstances {

  implicit def appendCoder[T: Coder: RedisType] = Coder.gen[Append[T]]
  implicit def setCoder[T: Coder: RedisType] = Coder.gen[Set[T]]
  implicit def incrByCoder[T: Coder: RedisType] = Coder.gen[IncrBy[T]]
  implicit def decrByCoder[T: Coder: RedisType] = Coder.gen[DecrBy[T]]
  implicit def sAddCoder[T: Coder: RedisType] = Coder.gen[SAdd[T]]
  implicit def lPushCoder[T: Coder: RedisType] = Coder.gen[LPush[T]]
  implicit def rPushCoder[T: Coder: RedisType] = Coder.gen[RPush[T]]
  implicit def pfAddCoder[T: Coder: RedisType] = Coder.gen[PFAdd[T]]

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

  implicit def redisMutationCoder[T <: RedisMutation] =
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
