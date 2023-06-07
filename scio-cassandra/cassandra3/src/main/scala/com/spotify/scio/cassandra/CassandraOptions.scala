package com.spotify.scio.cassandra

case class CassandraOptions(
  keyspace: String,
  table: String,
  cql: String,
  seedNodeHost: String,
  seedNodePort: Int = -1,
  username: String = null,
  password: String = null
)
