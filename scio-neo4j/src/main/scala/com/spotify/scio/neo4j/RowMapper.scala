package com.spotify.scio.neo4j

import org.neo4j.driver.Record

trait RowMapper[T] extends java.io.Serializable {
  def apply(record: Record): T
}

object RowMapper {
  def apply[T](implicit instance: RowMapper[T]): RowMapper[T] = instance
  def instance[T](f: Record => T): RowMapper[T] = f(_)
}
