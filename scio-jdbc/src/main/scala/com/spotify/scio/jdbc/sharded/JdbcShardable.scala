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

package com.spotify.scio.jdbc.sharded

import java.sql.ResultSet
import org.slf4j.LoggerFactory

trait JdbcShardable[T] extends Serializable {

  def columnValueDecoder(resultSet: ResultSet, columnName: String): T

  def partition(range: Range[T], numShards: Int): Seq[ShardQuery]

}

object ShardBy {

  object range {
    def of[T: RangeJdbcShardable]: JdbcShardable[T] = implicitly[RangeJdbcShardable[T]]
  }

  object prefix {
    def of[T](
      prefixLength: Int
    )(implicit shardeableF: Int => PrefixJdbcShardable[T]): JdbcShardable[T] =
      shardeableF(prefixLength)
  }

}

object JdbcShardable {

  implicit val longRangeJdbcShardable: RangeJdbcShardable[Long] = RangeJdbcShardable[Long](
    (rs, colName) => rs.getLong(colName),
    (range, numShards) => (range.upperBound - range.lowerBound) / numShards
  )

  implicit val intRangeJdbcShardable: RangeJdbcShardable[Int] = RangeJdbcShardable[Int](
    (rs, colName) => rs.getInt(colName),
    (range, numShards) => (range.upperBound - range.lowerBound) / numShards
  )

  implicit val decimalRangeJdbcShardable: RangeJdbcShardable[BigDecimal] =
    RangeJdbcShardable[BigDecimal](
      (rs, colName) => rs.getBigDecimal(colName),
      (range, numShards) => (range.upperBound - range.lowerBound) / numShards
    )

  implicit val doubleRangeJdbcShardable: RangeJdbcShardable[Double] =
    RangeJdbcShardable[Double](
      (rs, colName) => rs.getDouble(colName),
      (range, numShards) => (range.upperBound - range.lowerBound) / numShards
    )

  implicit val floatRangeJdbcShardable: RangeJdbcShardable[Float] =
    RangeJdbcShardable[Float](
      (rs, colName) => rs.getFloat(colName),
      (range, numShards) => (range.upperBound - range.lowerBound) / numShards
    )

  implicit val stringPrefixJdbcShardable: Int => PrefixJdbcShardable[String] = prefixLength =>
    new PrefixJdbcShardable[String](
      (rs, colName) => rs.getString(colName),
      // TODO: implement the proper string space partitiong, the empty string prefix effectively
      //  creates only a single shard
      range => Seq("")
    )

}

final class RangeJdbcShardable[T](
  decoder: (ResultSet, String) => T,
  partitionLength: (Range[T], Int) => T
)(implicit numeric: Numeric[T])
    extends JdbcShardable[T] {

  private val log = LoggerFactory.getLogger(this.getClass)

  def columnValueDecoder(resultSet: ResultSet, columnName: String): T =
    decoder(resultSet, columnName)

  def partition(range: Range[T], numShards: Int): Seq[ShardQuery] = {

    require(
      numeric.lt(range.lowerBound, range.upperBound) ||
        numeric.equiv(range.lowerBound, range.upperBound),
      "The lower bound of the range must be less than or equal to the upper bound"
    )

    val partitionsCount = numeric.min(
      numeric.max(numeric.minus(range.upperBound, range.lowerBound), numeric.one),
      numeric.max(numeric.one, numeric.fromInt(numShards))
    )

    log.info(
      ("Going to partition the read into %s ranges for lowerBound=%s, upperBound=%s, " +
        "numShard=%s").format(partitionsCount, range.lowerBound, range.upperBound, numShards)
    )

    val intPartitionsCount = numeric.toInt(partitionsCount)
    val partLength = partitionLength(range, intPartitionsCount)

    (0 until intPartitionsCount)
      .map { idx =>
        val lowerBound =
          if (idx == 0)
            range.lowerBound
          else
            numeric.plus(range.lowerBound, numeric.times(numeric.fromInt(idx), partLength))
        if (idx == intPartitionsCount - 1)
          RangeShardQuery(Range(lowerBound, range.upperBound), upperBoundInclusive = true)
        else
          RangeShardQuery(
            Range(
              lowerBound,
              numeric.plus(range.lowerBound, numeric.times(numeric.fromInt(idx + 1), partLength))
            ),
            upperBoundInclusive = false
          )
      }
      .map { query =>
        log.info(s"A query to read a partition of the table was produced [$query]")
        query
      }

  }

}

object RangeJdbcShardable {

  def apply[T: Numeric](decoder: (ResultSet, String) => T, partitionLength: (Range[T], Int) => T) =
    new RangeJdbcShardable[T](decoder, partitionLength)

}

final class PrefixJdbcShardable[T](
  decoder: (ResultSet, String) => T,
  partitioner: Range[T] => Seq[T]
) extends JdbcShardable[T] {

  def columnValueDecoder(resultSet: ResultSet, columnName: String): T =
    decoder(resultSet, columnName)

  def partition(range: Range[T], numShards: Int): Seq[ShardQuery] =
    partitioner(range).map(PrefixShardQuery(_))

}
