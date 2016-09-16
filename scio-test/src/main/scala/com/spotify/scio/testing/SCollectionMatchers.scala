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
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.util.ClosureCleaner
import com.spotify.scio.values.SCollection
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** Trait with ScalaTest [[Matcher]]s for [[SCollection]]s. */
trait SCollectionMatchers {

  private def m(f: () => Any): MatchResult = {
    val r = try { f(); true } catch { case NonFatal(_) => false }
    MatchResult(r, "", "")
  }

  // Due to  https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues/434
  // SerDe cycle on each element to keep consistent with values on the expected side
  private def serDeCycle[T: ClassTag](scollection: SCollection[T]): SCollection[T] = {
    val coder = scollection.internal.getCoder
    scollection
      .map(e => CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, e)))
  }

  def containInAnyOrder[T: ClassTag](value: Iterable[T])
  : Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult =
      m(() => DataflowAssert.that(serDeCycle(left).internal).containsInAnyOrder(value.asJava))
  }

  def containSingleValue[T: ClassTag](value: T)
  : Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult =
      m(() => DataflowAssert.thatSingleton(serDeCycle(left).internal).isEqualTo(value))
  }

  val beEmpty = new Matcher[SCollection[_]] {
    override def apply(left: SCollection[_]): MatchResult =
      m(() => DataflowAssert.that(left.internal).empty())
  }

  def haveSize(size: Int): Matcher[SCollection[_]] = new Matcher[SCollection[_]] {
    override def apply(left: SCollection[_]): MatchResult = {
      val g = new SerializableFunction[java.lang.Iterable[Any], Void] {
        val s = size  // defeat closure
        override def apply(input: JIterable[Any]): Void = {
          val inputSize = input.asScala.size
          assert(inputSize == s, s"SCollection had size $inputSize instead of expected size $s")
          null
        }
      }
      m(() => DataflowAssert.that(left.asInstanceOf[SCollection[Any]].internal).satisfies(g))
    }
  }

  def equalMapOf[K: ClassTag, V: ClassTag](value: Map[K, V])
  : Matcher[SCollection[(K, V)]] = new Matcher[SCollection[(K, V)]] {
    override def apply(left: SCollection[(K, V)]): MatchResult = {
      m(() => DataflowAssert.thatMap(serDeCycle(left).toKV.internal).isEqualTo(value.asJava))
    }
  }

  def notEqualMapOf[K: ClassTag, V: ClassTag](value: Map[K, V])
  : Matcher[SCollection[(K, V)]] = new Matcher[SCollection[(K, V)]] {
    override def apply(left: SCollection[(K, V)]): MatchResult = {
      m(() => DataflowAssert.thatMap(serDeCycle(left).toKV.internal).notEqualTo(value.asJava))
    }
  }

  // TODO: investigate why multi-map doesn't work

  def satisfy[T: ClassTag](predicate: Iterable[T] => Boolean)
  : Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult = {
      val f = ClosureCleaner(predicate)
      val g = new SerializableFunction[JIterable[T], Void] {
        override def apply(input: JIterable[T]): Void = {
          assert(f(input.asScala))
          null
        }
      }
      m(() => DataflowAssert.that(serDeCycle(left).internal).satisfies(g))
    }
  }

  def forAll[T: ClassTag](predicate: T => Boolean): Matcher[SCollection[T]] =
    satisfy(_.forall(predicate))

  def exist[T: ClassTag](predicate: T => Boolean): Matcher[SCollection[T]] =
    satisfy(_.exists(predicate))

}
