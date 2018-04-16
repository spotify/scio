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

package com.spotify.scio

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class ArrayMonoidTest extends FlatSpec with Matchers {

  val dimension = 10
  val mon = doubleArrayMon
  val zero = mon.zero
  def nextArray: Array[Double] = Array.fill(dimension)(Random.nextDouble)

  def plus(l: Array[Double], r: Array[Double]): Array[Double] = l.zip(r).map(p => p._1 + p._2)

  "ArrayMonoid" should "support plus" in {
    val l = nextArray
    val r = nextArray
    mon.plus(l, r) shouldBe plus(l, r)
    mon.plus(l, zero) shouldBe l
    mon.plus(zero, r) shouldBe r
    mon.plus(zero, zero) shouldBe zero
  }

  it should "support sumOption" in {
    mon.sumOption(Seq.empty[Array[Double]]) shouldBe None

    val a = nextArray
    mon.sumOption(Seq(a)).get shouldBe a

    val xs = (1 to 100).map(_ => nextArray)
    mon.sumOption(xs).get shouldBe xs.reduce(plus)

    mon.sumOption(zero +: xs).get shouldBe xs.reduce(plus)
    mon.sumOption(xs :+ zero).get shouldBe xs.reduce(plus)
    mon.sumOption(xs.take(50) ++ Seq(zero) ++ xs.takeRight(50)).get shouldBe xs.reduce(plus)
  }

}
