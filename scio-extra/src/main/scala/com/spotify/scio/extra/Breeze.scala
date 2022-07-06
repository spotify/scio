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

package com.spotify.scio.extra

import breeze.linalg.operators.OpAdd
import breeze.linalg.support.CanCopy
import com.twitter.algebird.Semigroup

import scala.collection.compat._ // scalafix:ok

/**
 * Utilities for Breeze.
 *
 * Includes [[com.twitter.algebird.Semigroup Semigroup]] s for Breeze data types like
 * [[breeze.linalg.DenseVector DenseVector]] s and [[breeze.linalg.DenseMatrix DenseMatrix]] s.
 *
 * {{{
 * import com.spotify.scio.extra.Breeze._
 *
 * val vectors: SCollection[DenseVector[Double]] = // ...
 * vectors.sum  // implicit Semigroup[T]
 * }}}
 */
object Breeze {
  implicit def breezeSemigroup[M[_], T](implicit
    add: OpAdd.Impl2[M[T], M[T], M[T]],
    addInto: OpAdd.InPlaceImpl2[M[T], M[T]],
    copy: CanCopy[M[T]]
  ): Semigroup[M[T]] =
    new Semigroup[M[T]] {
      override def plus(l: M[T], r: M[T]): M[T] = add(l, r)
      override def sumOption(xs: TraversableOnce[M[T]]): Option[M[T]] = {
        var s: M[T] = null.asInstanceOf[M[T]]
        val i = xs.iterator
        while (i.hasNext) {
          val a = i.next()
          if (s == null) {
            s = copy(a)
          } else {
            addInto(s, a)
          }
        }
        Option(s)
      }
    }
}
