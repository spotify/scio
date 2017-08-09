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

import com.spotify.scio.util.ClosureCleaner
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.util.CoderUtils
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Trait with ScalaTest [[org.scalatest.matchers.Matcher Matcher]]s for
 * [[com.spotify.scio.values.SCollection SCollection]]s.
 */
trait SCollectionMatchers {

  /*
  Wrapper for PAssert statements. PAssert does not perform assertions or throw exceptions until
  sc.close() is called. So MatchResult should always match true for "a should $Matcher" cases and
  false for "a shouldNot $Matcher" cases. We also need to run different assertions for positive
  (shouldFn) and negative (shouldNotFn) cases.
   */
  private def m(shouldFn: () => Any, shouldNotFn: () => Any): MatchResult = {
    val isShouldNot = Thread.currentThread()
      .getStackTrace
      .exists(e => e.getClassName.startsWith("org.scalatest.") && e.getMethodName == "shouldNot")
    val r = if (isShouldNot) {
      shouldNotFn()
      false
    } else {
      shouldFn()
      true
    }
    MatchResult(r, "", "")
  }

  private def makeFn[T](f: JIterable[T] => Unit): SerializableFunction[JIterable[T], Void] =
    new SerializableFunction[JIterable[T], Void] {
      override def apply(input: JIterable[T]) = {
        f(input)
        null
      }
    }

  // Due to  https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues/434
  // SerDe cycle on each element to keep consistent with values on the expected side
  private def serDeCycle[T: ClassTag](scollection: SCollection[T]): SCollection[T] = {
    val coder = scollection.internal.getCoder
    scollection
      .map(e => CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, e)))
  }

  /** Assert that the SCollection in question contains the provided elements. */
  def containInAnyOrder[T: ClassTag](value: Iterable[T])
  : Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult = {
      val v = value // defeat closure
      val f = makeFn[T] { in =>
        import org.hamcrest.Matchers
        import org.junit.Assert
        Assert.assertThat(in, Matchers.not(Matchers.containsInAnyOrder(v.toSeq: _*)))
      }
      m(
        () => PAssert.that(serDeCycle(left).internal).containsInAnyOrder(value.asJava),
        () => PAssert.that(serDeCycle(left).internal).satisfies(f))
    }
  }

  /** Assert that the SCollection in question contains a single provided element. */
  def containSingleValue[T: ClassTag](value: T)
  : Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult = m(
      () => PAssert.thatSingleton(serDeCycle(left).internal).isEqualTo(value),
      () => PAssert.thatSingleton(serDeCycle(left).internal).notEqualTo(value))
  }

  /** Assert that the SCollection in question is empty. */
  val beEmpty = new Matcher[SCollection[_]] {
    override def apply(left: SCollection[_]): MatchResult = m(
      () => PAssert.that(left.internal).empty(),
      () => PAssert.that(left.asInstanceOf[SCollection[Any]].internal).satisfies(makeFn(in =>
        assert(in.iterator().hasNext, "SCollection is empty"))))
  }

  /** Assert that the SCollection in question has provided size. */
  def haveSize(size: Int): Matcher[SCollection[_]] = new Matcher[SCollection[_]] {
    override def apply(left: SCollection[_]): MatchResult = {
      val s = size  // defeat closure
      val f = makeFn[Any] { in =>
        val inSize = in.asScala.size
        assert(inSize == s, s"SCollection expected size: $s, actual: $inSize")
      }
      val g = makeFn[Any] { in =>
        val inSize = in.asScala.size
        assert(inSize != s, s"SCollection expected size: not $s, actual: $inSize")
      }
      m(
        () => PAssert.that(left.asInstanceOf[SCollection[Any]].internal).satisfies(f),
        () => PAssert.that(left.asInstanceOf[SCollection[Any]].internal).satisfies(g))
    }
  }

  /** Assert that the SCollection in question is equivalent to the provided map. */
  def equalMapOf[K: ClassTag, V: ClassTag](value: Map[K, V])
  : Matcher[SCollection[(K, V)]] = new Matcher[SCollection[(K, V)]] {
    override def apply(left: SCollection[(K, V)]): MatchResult = m(
      () => PAssert.thatMap(serDeCycle(left).toKV.internal).isEqualTo(value.asJava),
      () => PAssert.thatMap(serDeCycle(left).toKV.internal).notEqualTo(value.asJava)
    )
  }

  // TODO: investigate why multi-map doesn't work

  /** Assert that the SCollection in question satisfies the provided function. */
  def satisfy[T: ClassTag](predicate: Iterable[T] => Boolean)
  : Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult = {
      val p = ClosureCleaner(predicate)
      val f = makeFn[T](in => assert(p(in.asScala)))
      val g = makeFn[T](in => assert(!p(in.asScala)))
      m(
        () => PAssert.that(serDeCycle(left).internal).satisfies(f),
        () => PAssert.that(serDeCycle(left).internal).satisfies(g))
    }
  }

  /** Assert that all elements of the SCollection in question satisfy the provided function. */
  def forAll[T: ClassTag](predicate: T => Boolean): Matcher[SCollection[T]] =
    satisfy(_.forall(predicate))

  /** Assert that some elements of the SCollection in question satisfy the provided function. */
  def exist[T: ClassTag](predicate: T => Boolean): Matcher[SCollection[T]] =
    satisfy(_.exists(predicate))

}
