/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.values

import com.spotify.scio._
import com.spotify.scio.util.StatCounter

/**
 * Extra functions available on SCollections of Doubles through an implicit conversion.
 */
class DoubleSCollectionFunctions(self: SCollection[Double]) {

  /**
   * Return an SCollection with a single [[com.spotify.scio.util.StatCounter
   * StatCounter]] object that captures the mean, variance and count of the SCollection's elements
   * in one operation.
   */
  def stats: SCollection[StatCounter] = self.combine(StatCounter(_))(_.merge(_))(_.merge(_))

  // Implemented in SCollection
  // def mean: SCollection[Double] = this.stats().map(_.mean)

  // Implemented in SCollection
  // def sum: SCollection[Double] = this.stats().map(_.sum)

  /** Compute the standard deviation of this SCollection's elements. */
  def stdev: SCollection[Double] = self.transform(_.stats.map(_.stdev))

  /** Compute the variance of this SCollection's elements. */
  def variance: SCollection[Double] = self.transform(_.stats.map(_.variance))

  /**
   * Compute the sample standard deviation of this SCollection's elements (which corrects for bias
   * in estimating the standard deviation by dividing by N-1 instead of N).
   */
  def sampleStdev: SCollection[Double] = self.transform(_.stats.map(_.sampleStdev))

  /**
   * Compute the sample variance of this SCollection's elements (which corrects for bias in
   * estimating the variance by dividing by N-1 instead of N).
   */
  def sampleVariance: SCollection[Double] = self.transform(_.stats.map(_.sampleVariance))

  // Ported from org.apache.spark.rdd.DoubleRDDFunctions

  /**
   * Compute a histogram of the data using `bucketCount` number of buckets evenly spaced between
   * the minimum and maximum of the SCollection. For example if the min value is 0 and the max is
   * 100 and there are two buckets the resulting buckets will be [0, 50) [50, 100]. `bucketCount`
   * must be at least 1. If the SCollection contains infinity, NaN throws an exception. If the
   * elements in SCollection do not vary (max == min) always returns a single bucket.
   */
  def histogram(bucketCount: Int): (SCollection[Array[Double]], SCollection[Array[Long]]) = {
    // Compute the minimum and the maximum
    val minMax = self.aggregate((Double.PositiveInfinity, Double.NegativeInfinity))(
      (acc, x) => (x.min(acc._1), x.max(acc._2)),
      (l, r) => (l._1.min(r._1), l._2.max(r._2)))
    val buckets = minMax.map { case (min, max) =>
      if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
        throw new UnsupportedOperationException(
          "Histogram on either an empty SCollection or SCollection containing +/-infinity or NaN")
      }
      val range = if (min != max) {
        // Range.Double.inclusive(min, max, increment)
        // The above code doesn't always work. See Scala bug #SI-8782.
        // https://issues.scala-lang.org/browse/SI-8782
        val span = max - min
        val steps = bucketCount
        Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
      } else {
        List(min, min)
      }
      range.toArray
    }
    (buckets, histogramImpl(buckets, true))
  }

  /**
   * Compute a histogram using the provided buckets. The buckets are all open to the right except
   * for the last which is closed e.g. for the array `[1, 10, 20, 50]` the buckets are `[1, 10)
   * [10, 20) [20, 50]` e.g `1<=x<10`, `10<=x<20`, `20<=x<=50`. And on the input of 1 and 50 we
   * would have a histogram of `[1, 0, 1]`.
   *
   * Note: if your histogram is evenly spaced (e.g. `[0, 10, 20, 30]`) this can be switched from
   * an O(log n) insertion to O(1) per element. (where n = # buckets) if you set `evenBuckets` to
   * true.
   *
   * buckets must be sorted and not contain any duplicates. buckets array must be at least two
   * elements. All NaN entries are treated the same. If you have a NaN bucket it must be the
   * maximum value of the last position and all NaN entries will be counted in that bucket.
   */
  def histogram(buckets: Array[Double], evenBuckets: Boolean = false): SCollection[Array[Long]] =
    histogramImpl(self.context.parallelize(Seq(buckets)), evenBuckets)

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  private def histogramImpl(buckets: SCollection[Array[Double]],
                            evenBuckets: Boolean = false): SCollection[Array[Long]] = {
    // Map buckets into a side input of bucket function
    val side = buckets.map { b =>
      require(b.length >= 2, "buckets array must have at least two elements")
      // Basic bucket function. This works using Java's built in Array
      // binary search. Takes log(size(buckets))
      def basicBucketFunction(e: Double): Option[Int] = {
        val location = java.util.Arrays.binarySearch(b, e)
        if (location < 0) {
          // If the location is less than 0 then the insertion point in the array
          // to keep it sorted is -location-1
          val insertionPoint = -location-1
          // If we have to insert before the first element or after the last one
          // its out of bounds.
          // We do this rather than buckets.lengthCompare(insertionPoint)
          // because Array[Double] fails to override it (for now).
          if (insertionPoint > 0 && insertionPoint < b.length) {
            Some(insertionPoint-1)
          } else {
            None
          }
        } else if (location < b.length - 1) {
          // Exact match, just insert here
          Some(location)
        } else {
          // Exact match to the last element
          Some(location - 1)
        }
      }

      // Determine the bucket function in constant time. Requires that buckets are evenly spaced
      def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int] = {
        // If our input is not a number unless the increment is also NaN then we fail fast
        if (e.isNaN || e < min || e > max) {
          None
        } else {
          // Compute ratio of e's distance along range to total range first, for better precision
          val bucketNumber = (((e - min) / (max - min)) * count).toInt
          // should be less than count, but will equal count if e == max, in which case
          // it's part of the last end-range-inclusive bucket, so return count-1
          Some(math.min(bucketNumber, count - 1))
        }
      }
      // Decide which bucket function to pass to histogramPartition. We decide here
      // rather than having a general function so that the decision need only be made
      // once rather than once per shard
      val bucketFunction = if (evenBuckets) {
        fastBucketFunction(b.head, b.last, b.length - 1) _
      } else {
        basicBucketFunction _
      }
      bucketFunction
    }.asSingletonSideInput

    val bucketSize = buckets.map(_.length - 1)
    val hist = self
      .withSideInputs(side)
      .flatMap { (x, c) =>
        // Map values to buckets
        val bucketFunction = c(side)
        bucketFunction(x).iterator
      }
      .toSCollection
      .countByValue  // Count occurrences of each bucket
      .cross(bucketSize)  // Replicate bucket size
      .map { case ((bin, count), size) =>
        val b = Array.fill(size)(0L)
        b(bin) = count
        b
      }
      .sum

    // Workaround since hist may be empty
    val bSide = bucketSize.asSingletonSideInput
    val hSide = hist.asListSideInput
    self.context.parallelize(Seq(0))
      .withSideInputs(bSide, hSide)
      .map { (z, c) =>
        val h = c(hSide)
        if (h.isEmpty) {
          Array.fill(c(bSide))(0L)
        } else {
          h.head
        }
      }
      .toSCollection
  }
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length

}
