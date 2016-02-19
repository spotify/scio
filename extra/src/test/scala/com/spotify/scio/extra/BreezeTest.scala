/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import breeze.stats.distributions.Rand
import com.spotify.scio.extra.Breeze._
import com.twitter.algebird.Semigroup
import org.scalatest.{Matchers, FlatSpec}

class BreezeTest extends FlatSpec with Matchers {

  val fRand = Rand.uniform.map(_.toFloat)

  val fvs = (1 to 10).map(_ => DenseVector.rand[Float](20, fRand))
  val fvSum = fvs.reduce(_ + _)
  val dvs = (1 to 10).map(_ => DenseVector.rand[Double](20))
  val dvSum = dvs.reduce(_ + _)

  val fms = (1 to 10).map(_ => DenseMatrix.rand[Float](20, 10, fRand))
  val fmSum = fms.reduce(_ + _)
  val dms = (1 to 10).map(_ => DenseMatrix.rand[Double](20, 10))
  val dmSum = dms.reduce(_ + _)

  "Semigroup" should "work on DenseVector[Float]" in {
    sum(fvs) should equal (fvSum)
    sumOption(fvs) should equal (Some(fvSum))
    sumOption(Iterable.empty[DenseVector[Float]]) should equal (None)
  }

  it should "work on DenseVector[Double]" in {
    sum(dvs) should equal (dvSum)
    sumOption(dvs) should equal (Some(dvSum))
    sumOption(Iterable.empty[DenseVector[Double]]) should equal (None)
  }

  it should "work on DenseMatrix[Float]" in {
    sum(fms) should equal (fmSum)
    sumOption(fms) should equal (Some(fmSum))
    sumOption(Iterable.empty[DenseMatrix[Float]]) should equal (None)
  }

  it should "work on DenseMatrix[Double]" in {
    sum(dms) should equal (dmSum)
    sumOption(dms) should equal (Some(dmSum))
    sumOption(Iterable.empty[DenseMatrix[Double]]) should equal (None)
  }

  def sum[T](xs: Iterable[T])(implicit sg: Semigroup[T]): T = xs.reduce(sg.plus)
  def sumOption[T](xs: Iterable[T])(implicit sg: Semigroup[T]): Option[T] = sg.sumOption(xs)

}
