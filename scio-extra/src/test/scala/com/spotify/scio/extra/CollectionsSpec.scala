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

import com.spotify.scio.extra.Collections._
import org.scalacheck.Prop.{BooleanOperators, all, forAll}
import org.scalacheck._

object CollectionsSpec extends Properties("Collections") {

  val posInts = Gen.posNum[Int]
  val intLists = Arbitrary.arbitrary[List[Int]]
  val tupleLists = Arbitrary.arbitrary[List[(String, Int)]]

  property("top") = forAll(intLists, posInts) { (xs, num) =>
    val maxExpected = xs.sorted.reverse.take(num).sorted
    val minExpected = xs.sorted.take(num).sorted
    def verify(actual: Iterable[Int], expected: List[Int]): Boolean =
      actual.toList.sorted == expected
    all(
      "List"       |: verify(xs.top(num), maxExpected),
      "Ordering"   |: verify(xs.top(num)(Ordering[Int].reverse), minExpected),
      "Array"      |: verify(xs.toArray.top(num), maxExpected),
      "Buffer"     |: verify(xs.toBuffer.top(num), maxExpected),
      "IndexedSeq" |: verify(xs.toIndexedSeq.top(num), maxExpected),
      "Iterable"   |: verify(xs.toIterable.top(num), maxExpected),
      "Seq"        |: verify(xs.toSeq.top(num), maxExpected),
      "Stream"     |: verify(xs.toStream.top(num), maxExpected),
      "Vector"     |: verify(xs.toVector.top(num), maxExpected)
    )
  }

  property("topByKey") = forAll(tupleLists, posInts) { (xs, num) =>
    val maxExpected = xs.groupBy(_._1).mapValues(_.map(_._2).sorted.reverse.take(num).sorted)
    val minExpected = xs.groupBy(_._1).mapValues(_.map(_._2).sorted.take(num).sorted)
    def verify(actual: Map[String, Iterable[Int]], expected: Map[String, List[Int]]): Boolean =
      actual.mapValues(_.toList.sorted) == expected
    all(
      "List"       |: verify(xs.topByKey(num), maxExpected),
      "Ordering"   |: verify(xs.topByKey(num)(Ordering[Int].reverse), minExpected),
      "Array"      |: verify(xs.toArray.topByKey(num), maxExpected),
      "IndexedSeq" |: verify(xs.toIndexedSeq.topByKey(num), maxExpected),
      "Iterable"   |: verify(xs.toIterable.topByKey(num), maxExpected),
      "Seq"        |: verify(xs.toSeq.topByKey(num), maxExpected),
      "Stream"     |: verify(xs.toStream.topByKey(num), maxExpected),
      "Vector"     |: verify(xs.toVector.topByKey(num), maxExpected)
    )
  }

}
