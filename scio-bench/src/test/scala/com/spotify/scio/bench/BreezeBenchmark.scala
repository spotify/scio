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

package com.spotify.scio.bench

import breeze.linalg.DenseVector
import com.spotify.scio.extra.Breeze._
import com.twitter.algebird.Semigroup
import org.scalameter.api._

/** Micro-benchmark for Breeze semigroups. */
object BreezeBenchmark extends Bench.LocalTime {

  val sizes = Gen.range("size")(200, 1000, 200)
  val inputs = for (s <- sizes) yield Array.fill(s)(DenseVector.rand[Double](100))
  val sg = implicitly[Semigroup[DenseVector[Double]]]

  performance of "Breeze" in {
    measure method "reduce" in {
      using(inputs) in { xs =>
        xs.reduce(_ + _)
      }
    }

    measure method "sumOption" in {
      using(inputs) in { xs =>
        sg.sumOption(xs).get
      }
    }
  }

}
