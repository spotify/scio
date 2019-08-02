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

package com.spotify

import com.twitter.algebird.Semigroup

import scala.reflect.ClassTag

/**
 * Main package for public APIs. Import all.
 *
 * {{{
 * import com.spotify.scio._
 * }}}
 */
package object scio {

  /** [[com.twitter.algebird.Semigroup Semigroup]] for `Array[Int]`. */
  implicit val intArraySg: Semigroup[Array[Int]] = new ArraySemigroup[Int]

  /** [[com.twitter.algebird.Semigroup Semigroup]] for `Array[Long]`. */
  implicit val longArraySg: Semigroup[Array[Long]] = new ArraySemigroup[Long]

  /** [[com.twitter.algebird.Semigroup Semigroup]] for `Array[Float]`. */
  implicit val floatArraySg: Semigroup[Array[Float]] = new ArraySemigroup[Float]

  /** [[com.twitter.algebird.Semigroup Semigroup]] for `Array[Double]`. */
  implicit val doubleArraySg: Semigroup[Array[Double]] = new ArraySemigroup[Double]

  private class ArraySemigroup[@specialized(Int, Long, Float, Double) T: ClassTag: Numeric]
      extends Semigroup[Array[T]] {

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
      require(l.length == r.length,  "Array lengths must be the same")
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
        if (a.length > 0) {
          if (s == null) {
            s = Array.fill[T](a.length)(num.zero)
          }
          plusI(s, a)
        }
      }
      Option(s)
    }

  }

}
