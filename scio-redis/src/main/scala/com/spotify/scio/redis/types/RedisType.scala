package com.spotify.scio.redis.types

sealed abstract class RedisType[T]

object RedisType {
  implicit case object StringRedisType extends RedisType[String]
  implicit case object ByteArrayRedisType extends RedisType[Array[Byte]]
}
