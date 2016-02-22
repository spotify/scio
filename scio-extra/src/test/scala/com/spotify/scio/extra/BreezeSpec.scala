/*
 * Copyright (c) 2016 Spotify AB.
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

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Rand
import com.spotify.scio.extra.Breeze._
import com.twitter.algebird.Semigroup
import org.scalacheck.Prop.forAll
import org.scalacheck._

import scala.language.higherKinds

trait BreezeSpec[M[_], T] {
  val dimension = 10
  val rows = 20
  val cols = 10
  val fRand = Rand.uniform.map(_.toFloat)
  val m: Gen[M[T]]
  def ms: Gen[Seq[M[T]]] = Gen.containerOf[Seq, M[T]](m)
  def plus(x: M[T], y: M[T])(implicit sg: Semigroup[M[T]]): M[T] = sg.plus(x, y)
  def sumOption(xs: Iterable[M[T]])(implicit sg: Semigroup[M[T]]): Option[M[T]] = sg.sumOption(xs)
}

object FVSemigroupSpec extends Properties("FloatVectorSemigroup") with BreezeSpec[DenseVector, Float] {
  val m = Gen.resultOf { _: Int => DenseVector.rand[Float](dimension, fRand) }
  property("plus") = forAll(m, m) { (x, y) =>
    plus(x, y) == x + y
  }
  property("sumOption") = forAll(ms) { xs =>
    sumOption(xs) == xs.reduceLeftOption(_ + _)
  }
}

object DVSemigroupSpec extends Properties("DoubleVectorSemigroup") with BreezeSpec[DenseVector, Double] {
  val m = Gen.resultOf { _: Int => DenseVector.rand(dimension) }
  property("plus") = forAll(m, m) { (x, y) =>
    plus(x, y) == x + y
  }
  property("sumOption") = Prop.forAll(ms) { xs =>
    sumOption(xs) == xs.reduceLeftOption(_ + _)
  }
}

object FMSemigroupSpec extends Properties("FloatMatrixSemigroup") with BreezeSpec[DenseMatrix, Float] {
  val m = Gen.resultOf { _: Int => DenseMatrix.rand[Float](rows, cols, fRand) }
  property("plus") = forAll(m, m) { (x, y) =>
    plus(x, y) == x + y
  }
  property("sumOption") = Prop.forAll(ms) { xs =>
    sumOption(xs) == xs.reduceLeftOption(_ + _)
  }
}

object DMSemigroupSpec extends Properties("DoubleMatrixSemigroup") with BreezeSpec[DenseMatrix, Double] {
  val m = Gen.resultOf { _: Int => DenseMatrix.rand[Double](rows, cols) }
  property("plus") = forAll(m, m) { (x, y) =>
    plus(x, y) == x + y
  }
  property("sumOption") = Prop.forAll(ms) { xs =>
    sumOption(xs) == xs.reduceLeftOption(_ + _)
  }
}
