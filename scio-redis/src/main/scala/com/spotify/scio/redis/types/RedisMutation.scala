package com.spotify.scio.redis.types

import org.joda.time.Duration

/**
 * Represents an abstract Redis command.
 * See Redis commands documentation for the description of commands: https://redis.io/commands
 */
sealed abstract class RedisMutation extends Serializable {
  def rt: RedisType[_]
}

object RedisMutation {
  def unapply[T <: RedisMutation](mt: T): Option[(T, RedisType[_])] = Some(mt -> mt.rt)
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
