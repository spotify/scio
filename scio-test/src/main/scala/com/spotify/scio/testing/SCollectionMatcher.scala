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

package com.spotify.scio.testing

import java.lang.{Iterable => JIterable}

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction
import com.spotify.scio.util.ClosureCleaner
import com.spotify.scio.values.SCollection
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[scio] trait SCollectionMatcher {

  private def tryAssert(f: () => Any): Boolean = {
    try {
      f()
      true
    } catch {
      case e: Throwable => false
    }
  }

  def containInAnyOrder[T](value: Iterable[T]): Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult =
      MatchResult(tryAssert(() => DataflowAssert.that(left.internal).containsInAnyOrder(value.asJava)), "", "")
  }

  def containSingleValue[T](value: T): Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult =
      MatchResult(tryAssert(() => DataflowAssert.thatSingleton(left.internal).isEqualTo(value)), "", "")
  }

  val beEmpty = new Matcher[SCollection[_]] {
    override def apply(left: SCollection[_]): MatchResult =
      MatchResult(tryAssert(() => DataflowAssert.that(left.internal).empty()), "", "")
  }

  def haveSize(size: Int): Matcher[SCollection[_]] = new Matcher[SCollection[_]] {
    override def apply(left: SCollection[_]): MatchResult = {
      val g = new SerializableFunction[java.lang.Iterable[Any], Void] {
        val s = size  // defeat closure
        override def apply(input: JIterable[Any]): Void = {
          assert(input.asScala.size == s)
          null
        }
      }
      MatchResult(tryAssert(() => DataflowAssert.that(left.asInstanceOf[SCollection[Any]].internal).satisfies(g)), "", "")
    }
  }

  def equalMapOf[K: ClassTag, V: ClassTag](value: Map[K, V]): Matcher[SCollection[(K, V)]] = new Matcher[SCollection[(K, V)]] {
    override def apply(left: SCollection[(K, V)]): MatchResult = {
      val kv = SCollection.makePairSCollectionFunctions(left).toKV.internal
      MatchResult(tryAssert(() => DataflowAssert.thatMap(kv).isEqualTo(value.asJava)), "", "")
    }
  }

  // TODO: investigate why multi-map doesn't work

  def forAll[T](predicate: T => Boolean): Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult = {
      val f = ClosureCleaner(predicate)
      val g = new SerializableFunction[JIterable[T], Void] {
        override def apply(input: JIterable[T]): Void = {
          assert(input.asScala.forall(f))
          null
        }
      }
      MatchResult(tryAssert(() => DataflowAssert.that(left.internal).satisfies(g)), "", "")
    }
  }

  def exist[T](predicate: T => Boolean): Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult = {
      val f = ClosureCleaner(predicate)
      val g = new SerializableFunction[JIterable[T], Void] {
        override def apply(input: JIterable[T]): Void = {
          assert(input.asScala.exists(f))
          null
        }
      }
      MatchResult(tryAssert(() => DataflowAssert.that(left.internal).satisfies(g)), "", "")
    }
  }

}
