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

package com.spotify.scio.extra

import breeze.linalg._
import breeze.linalg.operators.OpAdd
import breeze.linalg.support.CanCopy
import com.twitter.algebird.Monoid

import scala.language.higherKinds
import scala.reflect.ClassTag

/**
 * Utilities for Breeze.
 *
 * Includes [[com.twitter.algebird.Monoid Monoid]]s for [[breeze.linalg.DenseVector DenseVector]]s
 * and [[breeze.linalg.DenseMatrix DenseMatrix]]s of `Float` and `Double`.
 *
 * {{{
 * import com.spotify.scio.extra.Breeze._
 *
 * val vectors: SCollection[DenseVector[Double]] = // ...
 * vectors.sum  // implicit Monoid[T]
 * }}}
 */
object Breeze {

  // workaround for diverging implicit expansion
  private class BreezeOps[M](val zero: M,
                             val isNonZero: M => Boolean,
                             val copy: CanCopy[M],
                             val add: OpAdd.Impl2[M, M, M],
                             val addInPlace: OpAdd.InPlaceImpl2[M, M])

  private val floatDenseVectorOps = new BreezeOps(
    DenseVector.zeros[Float](0), (v: DenseVector[Float]) => v.length > 0,
    DenseVector.canCopyDenseVector[Float], DenseVector.canAddF, DenseVector.canAddIntoF)
  private val doubleDenseVectorOps = new BreezeOps(
    DenseVector.zeros[Double](0), (v: DenseVector[Double]) => v.length > 0,
    DenseVector.canCopyDenseVector[Double], DenseVector.canAddD, DenseVector.canAddIntoD)
  private val floatDenseMatrixOps = new BreezeOps(
    DenseMatrix.zeros[Float](0, 0), (v: DenseMatrix[Float]) => v.size > 0,
    DenseMatrix.canCopyDenseMatrix[Float],
    DenseMatrix.op_DM_DM_Float_OpAdd, DenseMatrix.dm_dm_UpdateOp_Float_OpAdd)
  private val doubleDenseMatrixOps = new BreezeOps(
    DenseMatrix.zeros[Double](0, 0), (v: DenseMatrix[Double]) => v.size > 0,
    DenseMatrix.canCopyDenseMatrix[Double],
    DenseMatrix.op_DM_DM_Double_OpAdd, DenseMatrix.dm_dm_UpdateOp_Double_OpAdd)

  private class BreezeMonoid[M[_], @specialized(Float, Double) T : ClassTag]
  (private val ops: BreezeOps[M[T]]) extends Monoid[M[T]] {

    override def isNonZero(v: M[T]): Boolean = ops.isNonZero(v)
    override def zero: M[T] = ops.zero

    override def plus(l: M[T], r: M[T]): M[T] =
      if (!isNonZero(l)) {
        r
      } else if (!isNonZero(r)) {
        l
      } else {
        ops.add(l, r)
      }

    override def sumOption(xs: TraversableOnce[M[T]]): Option[M[T]] = {
      var x: M[T] = null.asInstanceOf[M[T]]
      val i = xs.toIterator
      while (i.hasNext) {
        val y = i.next()
        if (x == null || !isNonZero(x)) {
          x = ops.copy(y)
        } else if (ops.isNonZero(y)) {
          ops.addInPlace(x, y)
        }
      }
      Option(x)
    }

  }

  implicit val floatDenseVectorMon: Monoid[DenseVector[Float]] =
    new BreezeMonoid(floatDenseVectorOps)
  implicit val doubleDenseVectorMon: Monoid[DenseVector[Double]] =
    new BreezeMonoid(doubleDenseVectorOps)
  implicit val floatDenseMatrixMon: Monoid[DenseMatrix[Float]] =
    new BreezeMonoid(floatDenseMatrixOps)
  implicit val doubleDenseMatrixMon: Monoid[DenseMatrix[Double]] =
    new BreezeMonoid(doubleDenseMatrixOps)

}
