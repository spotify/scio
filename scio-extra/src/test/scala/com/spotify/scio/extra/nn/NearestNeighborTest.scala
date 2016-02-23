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

package com.spotify.scio.extra.nn

import breeze.linalg._
import com.spotify.scio.extra.Collections._
import org.scalatest.{Matchers, FlatSpec}

object NearestNeighborTest {
  val numVectors = 1000
  val dimension = 40

  def newVec: DenseVector[Double] = {
    val v = DenseVector.rand[Double](dimension)
    v / norm(v)
  }
}

class NearestNeighborTest extends FlatSpec with NearestNeighborBehaviors {

  import NearestNeighborTest._

  override val testData = (1 to numVectors).map { i =>
    val v = DenseVector.rand[Double](dimension)
    ("k" + i, v :/ norm(v))
  }

  val matrixNN: NearestNeighbor[String, Double] = {
    val b = NearestNeighbor.newMatrixBuilder[String, Double](dimension)
    testData.foreach { case (k, v) => b.add(k, v) }
    b.build
  }

  val lshNN: NearestNeighbor[String, Double] = {
    val b = NearestNeighbor.newLSHBuilder[String, Double](dimension, 5, testData.size / 100)
    testData.foreach { case (k, v) => b.add(k, v) }
    b.build
  }

  "MatrixNearestNeighbor" should behave like aNN(matrixNN)
  "LSHNearestNeighbor" should behave like aNN(lshNN)

}

trait NearestNeighborBehaviors extends Matchers { this: FlatSpec =>

  import NearestNeighborTest._

  val testData: Seq[(String, DenseVector[Double])]

  def aNN(nn: NearestNeighbor[String, Double]): Unit = {

    it should "respect maxResult" in {
      Seq(1, 10, 100, 1000, 2000).foreach { maxResult =>
        nn.lookup(newVec, maxResult).size should be <= maxResult
      }
    }

    it should "respect minSimilarity" in {
      Seq(-1.0, -0.5, -0.1, 0.0, 0.1, 0.5, 1.0).foreach { minSimilarity =>
        nn.lookup(newVec, 100, minSimilarity).forall(_._2 >= minSimilarity) shouldBe true
      }
    }

    // TODO: figure out accuracy expectation
    it should "retrieve similar results" in {
      val v1 = newVec
      val expected = testData
        .map { case (k, v2) => (k, v1 dot v2) }
        .top(100)(Ordering.by(_._2))
      val actual = nn.lookup(v1, 100)
      (expected.map(_._1).toSet intersect actual.map(_._1).toSet).size should be >= 75
    }
  }

}
