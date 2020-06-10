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

trait Shard[T] extends Serializable {

  def columnValueDecoder(resultSet: ResultSet, columnName: String): T

  def partition(range: Range[T], numShards: Int): Seq[ShardQuery]

}

object ShardBy {

  object range {
    def of[T: RangeShard]: Shard[T] = implicitly[RangeShard[T]]

    object string {
      def of[T <: StringShard: RangeStringShard]: Shard[T] = implicitly[RangeStringShard[T]]
    }

  }

  object prefix {
    def of[T](
      prefixLength: Int
    )(implicit shardF: Int => PrefixShard[T]): Shard[T] = shardF(prefixLength)
  }

}

object Shard {

  implicit val longRangeJdbcShardable: RangeShard[Long] = RangeShard[Long](
    (rs, colName) => rs.getLong(colName),
    (range, numShards) => (range.upperBound - range.lowerBound) / numShards
  )

  implicit val intRangeJdbcShardable: RangeShard[Int] = RangeShard[Int](
    (rs, colName) => rs.getInt(colName),
    (range, numShards) => (range.upperBound - range.lowerBound) / numShards
  )

  implicit val decimalRangeJdbcShardable: RangeShard[BigDecimal] =
    RangeShard[BigDecimal](
      (rs, colName) => rs.getBigDecimal(colName),
      (range, numShards) => (range.upperBound - range.lowerBound) / numShards
    )

  implicit val doubleRangeJdbcShardable: RangeShard[Double] =
    RangeShard[Double](
      (rs, colName) => rs.getDouble(colName),
      (range, numShards) => (range.upperBound - range.lowerBound) / numShards
    )

  implicit val floatRangeJdbcShardable: RangeShard[Float] =
    RangeShard[Float](
      (rs, colName) => rs.getFloat(colName),
      (range, numShards) => (range.upperBound - range.lowerBound) / numShards
    )

  implicit val hexUpperStringJdbcShardable: RangeStringShard[StringShard.HexUpperString] =
    new RangeStringShard[StringShard.HexUpperString]

  implicit val hexLowerStringJdbcShardable: RangeStringShard[StringShard.HexLowerString] =
    new RangeStringShard[StringShard.HexLowerString]

  implicit val base64StringJdbcShardable: RangeStringShard[StringShard.Base64String] =
    new RangeStringShard[StringShard.Base64String]

}

final class RangeShard[T](
  decoder: (ResultSet, String) => T,
  partitionLength: (Range[T], Int) => T
)(implicit numeric: Numeric[T])
    extends Shard[T] {

  def columnValueDecoder(resultSet: ResultSet, columnName: String): T =
    decoder(resultSet, columnName)

  def partition(range: Range[T], numShards: Int): Seq[ShardQuery] =
    RangeShard.partition(range, numShards, partitionLength)

}

object RangeShard {
  private val log = LoggerFactory.getLogger(this.getClass)

  def partition[T: Numeric](
    range: Range[T],
    numShards: Int,
    partitionLength: (Range[T], Int) => T
  ): Seq[RangeShardQuery[T]] = {
    val numeric = implicitly[Numeric[T]]

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
          RangeShardQuery(
            Range(lowerBound, range.upperBound),
            upperBoundInclusive = true,
            quoteValues = false
          )
        else
          RangeShardQuery(
            Range(
              lowerBound,
              numeric.plus(range.lowerBound, numeric.times(numeric.fromInt(idx + 1), partLength))
            ),
            upperBoundInclusive = false,
            quoteValues = false
          )
      }
      .map { query =>
        log.info(s"A query to read a partition of the table was produced [$query]")
        query
      }

  }

  def apply[T: Numeric](decoder: (ResultSet, String) => T, partitionLength: (Range[T], Int) => T) =
    new RangeShard[T](decoder, partitionLength)

}

final class RangeStringShard[T <: StringShard](implicit
  rangeStringShardCoder: RangeStringShardCoder[T]
) extends Shard[T] {
  def columnValueDecoder(resultSet: ResultSet, columnName: String): T =
    rangeStringShardCoder.lift(resultSet.getString(columnName))

  def partition(range: Range[T], numShards: Int): Seq[ShardQuery] = {
    val lower = rangeStringShardCoder.decode(range.lowerBound)
    val upper = rangeStringShardCoder.decode(range.upperBound)

    RangeShard
      .partition[BigInt](
        Range(lower, upper),
        numShards,
        (rng, nShards) => (rng.upperBound - rng.lowerBound) / nShards
      )
      .map { rangeQuery =>
        rangeQuery.copy(
          range = Range(
            rangeStringShardCoder.encode(rangeQuery.range.lowerBound),
            rangeStringShardCoder.encode(rangeQuery.range.upperBound)
          ),
          quoteValues = true
        )
      }
  }
}

final class PrefixShard[T](
  decoder: (ResultSet, String) => T,
  partitioner: Range[T] => Seq[T]
) extends Shard[T] {

  def columnValueDecoder(resultSet: ResultSet, columnName: String): T =
    decoder(resultSet, columnName)

  def partition(range: Range[T], numShards: Int): Seq[ShardQuery] =
    partitioner(range).map(PrefixShardQuery(_))

}
