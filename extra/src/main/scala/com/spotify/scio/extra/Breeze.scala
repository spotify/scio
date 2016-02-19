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
import com.twitter.algebird.Semigroup

/** Utilities for Breeze. */
object Breeze {

  private type FV = DenseVector[Float]
  private type DV = DenseVector[Double]
  private type FM = DenseMatrix[Float]
  private type DM = DenseMatrix[Double]

  /** [[com.twitter.algebird.Semigroup Semigroup]] for DenseVector[Float]. */
  implicit val floatVectorSg: Semigroup[FV] = new Semigroup[FV] {
    override def plus(l: FV, r: FV): FV = l + r
    override def sumOption(iter: TraversableOnce[FV]): Option[FV] = {
      var x: FV = null
      iter.foreach { y =>
        if (x == null) {
          x = y.copy
        } else {
          x :+= y
        }
      }
      Option(x)
    }
  }

  /** [[com.twitter.algebird.Semigroup Semigroup]] for DenseVector[Double]. */
  implicit val doubleVectorSg: Semigroup[DV] = new Semigroup[DV] {
    override def plus(l: DV, r: DV): DV = l + r
    override def sumOption(iter: TraversableOnce[DV]): Option[DV] = {
      var x: DV = null
      iter.foreach { y =>
        if (x == null) {
          x = y.copy
        } else {
          x :+= y
        }
      }
      Option(x)
    }
  }

  /** [[com.twitter.algebird.Semigroup Semigroup]] for DenseMatrix[Float]. */
  implicit val floatMatrixSg: Semigroup[FM] = new Semigroup[FM] {
    override def plus(l: FM, r: FM): FM = l + r
    override def sumOption(iter: TraversableOnce[FM]): Option[FM] = {
      var x: FM = null
      iter.foreach { y =>
        if (x == null) {
          x = y.copy
        } else {
          x :+= y
        }
      }
      Option(x)
    }
  }

  /** [[com.twitter.algebird.Semigroup Semigroup]] for DenseMatrix[Double]. */
  implicit val doubleMatrixSg: Semigroup[DM] = new Semigroup[DM] {
    override def plus(l: DM, r: DM): DM = l + r
    override def sumOption(iter: TraversableOnce[DM]): Option[DM] = {
      var x: DM = null
      iter.foreach { y =>
        if (x == null) {
          x = y.copy
        } else {
          x :+= y
        }
      }
      Option(x)
    }
  }

}
