/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.cassandra

import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.ByteBuffer
import java.time.{Instant, LocalTime}
import java.util.{Date, UUID}

import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{Cluster, LocalDate, Row}
import com.spotify.scio._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class CassandraIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  import CassandraIT._

  private val n = 1000
  private val host = "127.0.0.1"

  // TODO: figure out why inet/InetAddress doesn't work
  private val table1Cql =
    """
      |CREATE TABLE scio.table1 (
      |  key text,          // String
      |  i1 tinyint,        // Byte
      |  i2 smallint,       // Short
      |  i3 int,            // Int
      |  i4 bigint,         // Long
      |  d decimal,         // java.math.BigDecimal
      |  f1 float,          // Float
      |  f2 double,         // Double
      |  b1 boolean,        // Boolean
      |  b2 blob,           // java.nio.ByteBuffer
      |  dt1 date,          // com.datastax.driver.core.LocalDate
      |  dt2 time,          // Long, the number of nanoseconds since midnight
      |  dt3 timestamp,     // java.util.Date
      |  u1 uuid,           // java.util.UUID
      |  u2 timeuuid,       // java.util.UUID, Version 1
      |  v1 varchar,        // String
      |  v2 varint,         // java.math.BigInteger
      |  c1 list<text>,     // java.util.List
      |  c2 set<text>,      // java.util.Set
      |  c3 map<text, int>, // java.util.Map
      |  PRIMARY KEY (key)
      |)
    """.stripMargin

  private val table2Cql =
    """
      |CREATE TABLE scio.table2 (
      |  k1 text,
      |  k2 text,
      |  k3 text,
      |  v1 text,
      |  v2 int,
      |  v3 float,
      |  PRIMARY KEY (k1, k2, k3)
      |);
    """.stripMargin

  private def connect(): Cluster = Cluster.builder().addContactPoint(host).build()

  override protected def beforeAll(): Unit = {
    val cluster = connect()
    try {
      val session = cluster.connect()
      if (cluster.getMetadata.getKeyspace("scio") != null) {
        session.execute("DROP KEYSPACE scio")
      }
      session.execute("""
          |CREATE KEYSPACE scio
          |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """.stripMargin)
      session.execute(table1Cql)
      session.execute(table2Cql)
    } catch {
      case e: Throwable =>
        cluster.close()
        throw e
    }
    ()
  }

  ignore should "work with single key" in {
    val cql =
      """
        |INSERT INTO scio.table1
        |(key, i1, i2, i3, i4, d, f1, f2, b1, b2, dt1, dt2, dt3, u1, u2, v1, v2, c1, c2, c3)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """.stripMargin
    val opts = CassandraOptions("scio", "table1", cql, host)

    val cluster = connect()
    try {
      val sc = ScioContext()
      sc.parallelize(1 to n)
        .saveAsCassandra(opts)(toValues1)
      sc.run()

      val result = cluster.connect().execute("SELECT * FROM scio.table1").asScala
      val expected = (1 to n).map(toValues1)
      result.map(fromRow1) should contain theSameElementsAs expected
    } finally {
      cluster.close()
      CassandraUtil.cleanup()
    }
  }

  ignore should "work with composite key" in {
    val cql =
      """
        |INSERT INTO scio.table2
        |(k1, k2, k3, v1, v2, v3) VALUES (?, ?, ?, ?, ?, ?);
      """.stripMargin
    val opts = CassandraOptions("scio", "table2", cql, host)

    val cluster = connect()
    try {
      val sc = ScioContext()
      sc.parallelize(1 to n)
        .saveAsCassandra(opts)(toValues2)
      sc.run()

      val result = cluster.connect().execute("SELECT * FROM scio.table2").asScala
      val expected = (1 to n).map(toValues2)
      result.map(fromRow2) should contain theSameElementsAs expected
    } finally {
      cluster.close()
      CassandraUtil.cleanup()
    }
  }
}

object CassandraIT {
  private val u1: UUID = UUID.randomUUID()
  private val u2: UUID = UUIDs.timeBased()
  private val date = LocalDate.fromMillisSinceEpoch(System.currentTimeMillis())
  private val time = LocalTime.now().toNanoOfDay
  private val timestamp = Date.from(Instant.now())

  def toValues1(i: Int): Seq[Any] =
    Seq(
      s"key$i",
      i.toByte,
      i.toShort,
      i,
      i.toLong,
      JBigDecimal.valueOf(i.toLong),
      i.toFloat,
      i.toDouble,
      i % 2 == 0,
      ByteBuffer.wrap(s"blob$i".getBytes),
      date,
      time,
      timestamp,
      u1,
      u2,
      s"varchar$i",
      BigInteger.valueOf(i.toLong),
      List(s"list$i").asJava,
      Set(s"set$i").asJava,
      Map(s"key$i" -> i).asJava
    )

  def fromRow1(r: Row): Seq[Any] =
    Seq(
      r.getString("key"),
      r.getByte("i1"),
      r.getShort("i2"),
      r.getInt("i3"),
      r.getLong("i4"),
      r.getDecimal("d"),
      r.getFloat("f1"),
      r.getDouble("f2"),
      r.getBool("b1"),
      r.getBytes("b2"),
      r.getDate("dt1"),
      r.getTime("dt2"),
      r.getTimestamp("dt3"),
      r.getUUID("u1"),
      r.getUUID("u2"),
      r.getString("v1"),
      r.getVarint("v2"),
      r.getList("c1", classOf[String]),
      r.getSet("c2", classOf[String]),
      r.getMap("c3", classOf[String], classOf[java.lang.Integer])
    )

  def toValues2(i: Int): Seq[Any] = Seq(s"k1_$i", s"k2_$i", s"k3_$i", s"v1_$i", i, i.toFloat)

  def fromRow2(r: Row): Seq[Any] =
    Seq(
      r.getString("k1"),
      r.getString("k2"),
      r.getString("k3"),
      r.getString("v1"),
      r.getInt("v2"),
      r.getFloat("v3")
    )
}
