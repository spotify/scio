/*
 * Copyright 2019 Spotify AB.
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

import com.spotify.scio.extra.Collections._
import org.scalacheck._
import org.scalatest._

import scala.collection.compat._ // scalafix:ok

class CollectionsSpec extends PropertySpec {
  val posInts: Gen[Int] = Gen.posNum[Int]
  val intLists: Gen[List[Int]] = Arbitrary.arbitrary[List[Int]]
  val tupleLists: Gen[List[(String, Int)]] = Arbitrary.arbitrary[List[(String, Int)]]

  property("top") {
    forAll(intLists, posInts) { (xs, num) =>
      val maxExpected = xs.sorted.reverse.take(num).sorted
      val minExpected = xs.sorted.take(num).sorted
      def verify(actual: Iterable[Int], expected: List[Int]): Assertion =
        actual.toList.sorted shouldBe expected

      verify(xs.top(num), maxExpected)
      verify(xs.top(num)(Ordering[Int].reverse), minExpected)
      verify(xs.to(Array).top(num), maxExpected)
      verify(xs.to(Iterable).top(num), maxExpected)
    }
  }

  property("topByKey") {
    forAll(tupleLists, posInts) { (xs, num) =>
      val maxExpected =
        xs.groupBy(_._1)
          .iterator
          .map { case (k, v) => k -> v.map(_._2).sorted.reverse.take(num).sorted }
          .toMap
      val minExpected =
        xs.groupBy(_._1)
          .iterator
          .map { case (k, v) => k -> v.map(_._2).sorted.take(num).sorted }
          .toMap
      def verify(actual: Map[String, Iterable[Int]], expected: Map[String, List[Int]]): Assertion =
        actual.iterator.map { case (k, v) => k -> v.toList.sorted }.toMap shouldBe expected
      verify(xs.topByKey(num), maxExpected)
      verify(xs.topByKey(num)(Ordering[Int].reverse), minExpected)
      verify(xs.to(Array).topByKey(num), maxExpected)
      verify(xs.to(Iterable).topByKey(num), maxExpected)
    }
  }
}
