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

import com.spotify.scio.extra.Collections._
import org.scalatest.{Matchers, FlatSpec}

class CollectionsTest extends FlatSpec with Matchers {

  val xs = 1 to 100
  val kvs = Seq("a", "b", "c").flatMap(k => xs.map((k, _)))
  val max5 = (96 to 100).toSet
  val min5 = (1 to 5).toSet

  "top" should "work on Array" in {
    xs.toArray.top(5).toSet should equal (max5)
    xs.toArray.top(5)(Ordering[Int].reverse).toSet should equal (min5)
    xs.toArray.top(200).toSet should equal (xs.toSet)
  }

  it should "work on Iterable" in {
    xs.toIterable.top(5).toSet should equal (max5)
    xs.toIterable.top(5)(Ordering[Int].reverse).toSet should equal (min5)
    xs.toIterable.top(200).toSet should equal (xs.toSet)
  }

  it should "work on all types of Iterable" in {
    xs.toBuffer.top(5).toSet should equal (max5)
    xs.toIndexedSeq.top(5).toSet should equal (max5)
    xs.toIterable.top(5).toSet should equal (max5)
    xs.toList.top(5).toSet should equal (max5)
    xs.toSeq.top(5).toSet should equal (max5)
    xs.toSet.top(5).toSet should equal (max5)
    xs.toStream.top(5).toSet should equal (max5)
    xs.toVector.top(5).toSet should equal (max5)
  }

  "topByKey" should "work on Array" in {
    verify(kvs.toArray.topByKey(5), max5)
    verify(kvs.toArray.topByKey(5)(Ordering[Int].reverse), min5)
    verify(kvs.toArray.topByKey(200), xs.toSet)
  }

  it should "work on Iterable" in {
    verify(kvs.toIterable.topByKey(5), max5)
    verify(kvs.toIterable.topByKey(5)(Ordering[Int].reverse), min5)
    verify(kvs.toIterable.topByKey(200), xs.toSet)
  }

  it should "work on other types of Iterable" in {
    verify(kvs.toIndexedSeq.topByKey(5), max5)
    verify(kvs.toIterable.topByKey(5), max5)
    verify(kvs.toList.topByKey(5), max5)
    verify(kvs.toSeq.topByKey(5), max5)
    verify(kvs.toStream.topByKey(5), max5)
    verify(kvs.toVector.topByKey(5), max5)
  }

  def verify(result: Map[String, Iterable[Int]], expectedValues: Set[Int]): Unit = {
    result.keySet should equal (Set("a", "b", "c"))
    result.values.foreach(_.toSet should equal (expectedValues))
  }

}
