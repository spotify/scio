package com.spotify.scio.neo4j

import org.neo4j.driver.Record

/**
 * Type class for converting a Neo4j query result [[org.neo4j.driver.Record]] to the desired type.
 */
trait RowMapper[T] extends Serializable {
  def apply(record: Record): T
}

object RowMapper {
  def apply[T](implicit instance: RowMapper[T]): RowMapper[T] = instance
}
