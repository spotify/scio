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

package com.spotify

import com.spotify.scio.io.Tap
import com.spotify.scio.values.AccumulatorType
import com.twitter.algebird.Semigroup
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Main package for public APIs. Import all.
 *
 * {{{
 * import com.spotify.scio._
 * }}}
 */
package object scio {

  /** Alias for BigQuery CreateDisposition. */
  val CREATE_IF_NEEDED = Write.CreateDisposition.CREATE_IF_NEEDED

  /** Alias for BigQuery CreateDisposition. */
  val CREATE_NEVER = Write.CreateDisposition.CREATE_NEVER

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_APPEND = Write.WriteDisposition.WRITE_APPEND

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_EMPTY = Write.WriteDisposition.WRITE_EMPTY

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_TRUNCATE = Write.WriteDisposition.WRITE_TRUNCATE

  /** Alias for WindowingStrategy AccumulationMode.ACCUMULATING_FIRED_PANES. */
  val ACCUMULATING_FIRED_PANES = AccumulationMode.ACCUMULATING_FIRED_PANES

  /** Alias for WindowingStrategy AccumulationMode.DISCARDING_FIRED_PANES. */
  val DISCARDING_FIRED_PANES = AccumulationMode.DISCARDING_FIRED_PANES

  import com.spotify.scio.values
  implicit val intAccumulatorType: AccumulatorType[Int] = new values.IntAccumulatorType
  implicit val longAccumulatorType: AccumulatorType[Long] = new values.LongAccumulatorType
  implicit val doubleAccumulatorType: AccumulatorType[Double] = new values.DoubleAccumulatorType

  implicit def makeDistCacheScioContext(self: ScioContext): DistCacheScioContext =
    new DistCacheScioContext(self)

  /** Wait for Tap to be available and get Tap reference from Future. */
  implicit class WaitableFutureTap[T](self: Future[Tap[T]]) {
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] = Await.result(self, atMost)
  }

  /** Wait for nested Tap to be available, flatten result and get Tap reference from Future. */
  implicit class WaitableNestedFutureTap[T](self: Future[Future[Tap[T]]]) {
    import scala.concurrent.ExecutionContext.Implicits.global
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] =
      Await.result(self.flatMap(identity), atMost)
  }

  /** Get Scio version from scio-core/src/main/resources/version.sbt. */
  def scioVersion: String = {
    val stream = this.getClass.getResourceAsStream("/version.sbt")
    val line = scala.io.Source.fromInputStream(stream).getLines().next()
    """version in .+"([^"]+)"""".r.findFirstMatchIn(line).get.group(1)
  }

  /** [[com.twitter.algebird.Semigroup Semigroup]] for Array[Int]. */
  implicit val intArraySg: Semigroup[Array[Int]] = new ArraySemigroup[Int]

  /** [[com.twitter.algebird.Semigroup Semigroup]] for Array[Long]. */
  implicit val longArraySg: Semigroup[Array[Long]] = new ArraySemigroup[Long]

  /** [[com.twitter.algebird.Semigroup Semigroup]] for Array[Float]. */
  implicit val floatArraySg: Semigroup[Array[Float]] = new ArraySemigroup[Float]

  /** [[com.twitter.algebird.Semigroup Semigroup]] for Array[Double]. */
  implicit val doubleArraySg: Semigroup[Array[Double]] = new ArraySemigroup[Double]

  class ArraySemigroup[@specialized(Int, Long, Float, Double) T : ClassTag : Numeric]
    extends Semigroup[Array[T]]{
    private val num = implicitly[Numeric[T]]
    private def plusI(l: Array[T], r: Array[T]): Array[T] = {
      require(l.length == r.length, "Array lengths do not match")
      import num.mkNumericOps
      var i = 0
      while (i < l.length) {
        l(i) += r(i)
        i += 1
      }
      l
    }
    override def plus(l: Array[T], r: Array[T]): Array[T] = {
      val s = Array.fill[T](l.length)(num.zero)
      plusI(s, l)
      plusI(s, r)
      s
    }
    override def sumOption(iter: TraversableOnce[Array[T]]): Option[Array[T]] = {
      var s: Array[T] = null
      iter.foreach { a =>
        if (s == null) {
          s = Array.fill[T](a.length)(num.zero)
        }
        plusI(s, a)
      }
      Option(s)
    }
  }

}
