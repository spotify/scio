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
import com.twitter.algebird.Monoid

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

  import com.spotify.scio.values
  implicit val intAccumulatorType: AccumulatorType[Int] = new values.IntAccumulatorType
  implicit val longAccumulatorType: AccumulatorType[Long] = new values.LongAccumulatorType
  implicit val doubleAccumulatorType: AccumulatorType[Double] = new values.DoubleAccumulatorType

  /**
   * Wait for [[com.spotify.scio.io.Tap Tap]] to be available and get Tap reference from `Future`.
   */
  implicit class WaitableFutureTap[T](self: Future[Tap[T]]) {
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] = Await.result(self, atMost)
  }

  /**
   * Wait for nested [[com.spotify.scio.io.Tap Tap]] to be available, flatten result and get Tap
   * reference from `Future`.
   */
  implicit class WaitableNestedFutureTap[T](self: Future[Future[Tap[T]]]) {
    import scala.concurrent.ExecutionContext.Implicits.global
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] =
      Await.result(self.flatMap(identity), atMost)
  }

  /** Scala version. */
  val scalaVersion: String = scala.util.Properties.versionNumberString

  /** Scio version. */
  val scioVersion: String = {
    val stream = this.getClass.getResourceAsStream("/version.sbt")
    val line = scala.io.Source.fromInputStream(stream).getLines().next()
    """version in .+"([^"]+)"""".r.findFirstMatchIn(line).get.group(1)
  }

  /** [[com.twitter.algebird.Monoid Monoid]] for `Array[Int]`. */
  implicit val intArrayMon: Monoid[Array[Int]] = new ArrayMonoid[Int]

  /** [[com.twitter.algebird.Monoid Monoid]] for `Array[Long]`. */
  implicit val longArrayMon: Monoid[Array[Long]] = new ArrayMonoid[Long]

  /** [[com.twitter.algebird.Monoid Monoid]] for `Array[Float]`. */
  implicit val floatArrayMon: Monoid[Array[Float]] = new ArrayMonoid[Float]

  /** [[com.twitter.algebird.Monoid Monoid]] for `Array[Double]`. */
  implicit val doubleArrayMon: Monoid[Array[Double]] = new ArrayMonoid[Double]

  private class ArrayMonoid[@specialized(Int, Long, Float, Double) T : ClassTag : Numeric]
    extends Monoid[Array[T]]{

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

    override def isNonZero(v: Array[T]): Boolean = v.nonEmpty
    override def zero: Array[T] = Array.empty[T]

    override def plus(l: Array[T], r: Array[T]): Array[T] =
      if (l.isEmpty) {
        r
      } else if (r.isEmpty) {
        l
      } else {
        val s = Array.fill[T](l.length)(num.zero)
        plusI(s, l)
        plusI(s, r)
        s
      }

    override def sumOption(xs: TraversableOnce[Array[T]]): Option[Array[T]] = {
      var s: Array[T] = null
      val i = xs.toIterator
      while (i.hasNext) {
        val a = i.next()
        if (s == null) {
          s = Array.fill[T](a.length)(num.zero)
        }
        if (a.nonEmpty) {
          plusI(s, a)
        }
      }
      Option(s)
    }

  }

}
