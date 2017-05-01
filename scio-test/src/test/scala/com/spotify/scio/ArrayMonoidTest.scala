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

package com.spotify.scio

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class ArrayMonoidTest extends FlatSpec with Matchers {

  val dimension = 10
  val m = doubleArrayMon
  def nextArray(empty: Boolean = false): Array[Double] =
    if (empty) {
      Array.emptyDoubleArray
    } else {
      Array.fill(dimension)(Random.nextDouble)
    }

  def plus(l: Array[Double], r: Array[Double]): Array[Double] =
    if (l.isEmpty) {
      r
    } else if (r.isEmpty) {
      l
    } else {
      l.zip(r).map(p => p._1 + p._2)
    }

  "ArrayMonoid" should "support plus" in {
    val l = nextArray()
    val r = nextArray()
    val e = nextArray(true)
    m.plus(l, r) should equal (plus(l, r))
    m.plus(l, e) should equal (l)
    m.plus(e, r) should equal (r)
  }

  it should "support sumOption" in {
    m.sumOption(Seq.empty[Array[Double]]) shouldBe None

    val a = nextArray()
    val e = nextArray(true)

    m.sumOption(Seq(a)).get should equal (a)
    m.sumOption(Seq(e)).get should equal (e)

    val xs = (1 to 100).map(x => nextArray(x % 10 == 0))
    m.sumOption(xs).get should equal (xs.reduce(plus))

    m.sumOption(Seq.fill(10)(e)).get should equal (e)
  }

}
