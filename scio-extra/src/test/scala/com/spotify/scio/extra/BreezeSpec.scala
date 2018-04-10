/*
 * Copyright 2018 Spotify AB.
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

import breeze.linalg.{DenseMatrix, DenseVector, SparseVector}
import breeze.stats.distributions.Rand
import com.spotify.scio.extra.Breeze._
import com.twitter.algebird.Semigroup
import org.scalacheck._

import scala.language.higherKinds

trait BreezeSpec[M[_], T] extends PropertySpec {
  val dimension = 10
  val rows = 20
  val cols = 10
  val fRand = Rand.uniform.map(_.toFloat)
  val m: Gen[M[T]]
  def ms: Gen[List[M[T]]] = Gen.listOf[M[T]](m)
  def plus(x: M[T], y: M[T])(implicit sg: Semigroup[M[T]]): M[T] = sg.plus(x, y)
  def sumOption(xs: Iterable[M[T]])(implicit sg: Semigroup[M[T]]): Option[M[T]] = sg.sumOption(xs)
}

class FloatDenseVectorSpec extends BreezeSpec[DenseVector, Float] {
  val m = Gen.const(dimension).map(DenseVector.rand[Float](_, fRand))

  property("plus") {
    forAll(m, m) { (x, y) =>
      plus(x, y) == x + y
    }
  }
  property("sumOption") {
    forAll(ms) { xs =>
      sumOption(xs) == xs.reduceLeftOption(_ + _)
    }
  }
}

class DoubleDenseVectorSpec extends BreezeSpec[DenseVector, Double] {
  val m = Gen.const(dimension).map(DenseVector.rand[Double](_))
  property("plus") {
    forAll(m, m) { (x, y) =>
      plus(x, y) == x + y
    }
  }
  property("sumOption") {
    forAll(ms) { xs =>
      sumOption(xs) == xs.reduceLeftOption(_ + _)
    }
  }
}

class FloatDenseMatrixSpec extends BreezeSpec[DenseMatrix, Float] {
  val m = Gen.const((rows, cols)).map { case (r, c) => DenseMatrix.rand[Float](r, c, fRand) }
  property("plus") {
    forAll(m, m) { (x, y) =>
      plus(x, y) == x + y
    }
  }
  property("sumOption") {
    forAll(ms) { xs =>
      sumOption(xs) == xs.reduceLeftOption(_ + _)
    }
  }
}

class DoubleDenseMatrixSpec extends BreezeSpec[DenseMatrix, Double] {
  val m = Gen.const((rows, cols)).map { case (r, c) => DenseMatrix.rand[Double](r, c) }
  property("plus") {
    forAll(m, m) { (x, y) =>
      plus(x, y) == x + y
    }
  }
  property("sumOption") {
    forAll(ms) { xs =>
      sumOption(xs) == xs.reduceLeftOption(_ + _)
    }
  }
}

class FloatSparseVectorSpec extends BreezeSpec[SparseVector, Float] {
  val m = Gen.const(dimension).map(d => SparseVector(DenseVector.rand[Float](d, fRand).data))

  property("plus") {
    forAll(m, m) { (x, y) =>
      plus(x, y) == x + y
    }
  }
  property("sumOption") {
    forAll(ms) { xs =>
      sumOption(xs) == xs.reduceLeftOption(_ + _)
    }
  }
}

class DoubleSparseVectorSpec extends BreezeSpec[SparseVector, Double] {
  val m = Gen.const(dimension).map(d => SparseVector(DenseVector.rand[Double](d).data))

  property("plus") {
    forAll(m, m) { (x, y) =>
      plus(x, y) == x + y
    }
  }
  property("sumOption") {
    forAll(ms) { xs =>
      sumOption(xs) == xs.reduceLeftOption(_ + _)
    }
  }
}
