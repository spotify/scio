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

package com.spotify.scio.extra.nn

import breeze.linalg._
import com.spotify.scio.extra.Collections._
import com.spotify.scio.extra.PropertySpec
import org.scalacheck._

class NearestNeighborSpec extends PropertySpec {

  val dimension = 40
  private def randVec = DenseVector.rand[Double](dimension)
  val vector = Gen.resultOf { _: Int => randVec }
  val vecs = Gen.nonEmptyListOf(vector).map(_.zipWithIndex.map(kv => ("key" + kv._2, kv._1)))
  val maxResult = Gen.posNum[Int]
  val minSims = Gen.chooseNum[Double](-1.0, 1.0)

  property("MatrixNN") {
    forAll(vecs, maxResult, minSims) { (vectors, maxResult, minSimilarity) =>
      val b = NearestNeighbor.newMatrixBuilder[String, Double](dimension)
      verify(b, vectors, maxResult, minSimilarity, 1.0, 1.0, 1.0)
    }
  }

  property("LSHNN") {
    forAll(vecs, maxResult, minSims) { (vectors, maxResult, minSimilarity) =>
      // TODO: figure out stage, bucket settings and coverage expectation
      val stages = 10
      val buckets = max(vectors.size / 100, 10)
      val b = NearestNeighbor.newLSHBuilder[String, Double](dimension, stages, buckets)
      verify(b, vectors, maxResult, minSimilarity, 0.5, 0.5, 0.5)
    }
  }

  private def verify(builder: NearestNeighborBuilder[String, Double],
                     vectors: List[(String, DenseVector[Double])],
                     maxResult: Int,
                     minSimilarity: Double,
                     minPrecision: Double, minRecall: Double, minF1: Double) = {
    vectors.foreach(kv => builder.add(kv._1, kv._2))
    val nn = builder.build
    nn.lookup(randVec, maxResult).size should be <= maxResult
    nn.lookup(randVec, 100, minSimilarity).forall(_._2 >= minSimilarity) shouldBe true
    coverage(vectors, nn, minPrecision, minRecall, minF1)
  }

  private def coverage(vectors: List[(String, DenseVector[Double])],
                       nn: NearestNeighbor[String, Double],
                       minPrecision: Double, minRecall: Double, minF1: Double) = {
    val v = randVec
    val expected = vectors
      .map(kv => (kv._1, kv._2 dot v))
      .top(100)(Ordering.by(_._2))
      .map(_._1)
      .toSet
    val actual = nn.lookup(v, 100).map(_._1).toSet
    val hits = (expected intersect actual).size.toDouble
    val precision = if (actual.nonEmpty) hits / actual.size else 1.0
    val recall = if (expected.nonEmpty) hits / expected.size else 1.0
    val f1 = 2 * precision * recall / (precision + recall)
    precision should be >= minPrecision
    minRecall should be >= minRecall
    f1 should be >= minF1
  }

}
