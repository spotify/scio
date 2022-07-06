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

package com.spotify.scio.testing

import java.lang.{Iterable => JIterable}
import java.util.{Map => JMap}

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.twitter.chill.Externalizer
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.PAssert.{IterableAssert, SingletonAssert}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.util.CoderUtils
import org.scalatest.matchers.{MatchResult, Matcher}
import org.{hamcrest => h}
import org.hamcrest.Matchers
import org.hamcrest.MatcherAssert.assertThat

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import com.twitter.chill.ClosureCleaner
import cats.kernel.Eq
import org.apache.beam.sdk.testing.SerializableMatchers
import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.ScioContext
import org.apache.beam.sdk.testing.SerializableMatcher

final private case class TestWrapper[T: Eq](get: T) {

  override def toString: String = Pretty.printer.apply(get).render

  override def equals(other: Any): Boolean =
    other match {
      case TestWrapper(o: T @unchecked) => Eq[T].eqv(get, o)
      case o                            => get == o
    }
}

private object TestWrapper {

  def wrap[T: Coder: Eq](coll: SCollection[T]): SCollection[TestWrapper[T]] =
    coll.map(t => TestWrapper(t))

  def wrap[T: Eq](coll: JIterable[T]): JIterable[TestWrapper[T]] =
    coll.asScala.map(t => TestWrapper(t)).asJava

  implicit def testWrapperCoder[T: Coder: Eq]: Coder[TestWrapper[T]] =
    Coder.xmap(Coder[T])(t => TestWrapper(t), w => w.get)
}

private object ScioMatchers {

  /** Create a hamcrest matcher that can be serialized using a Coder[T]. */
  private def supplierFromCoder[A: Coder, B](@transient a: A, @transient context: ScioContext)(
    builder: A => B
  ) = {
    val coder = CoderMaterializer.beam(context, Coder[A])
    val encoded = CoderUtils.encodeToByteArray(coder, a)
    new SerializableMatchers.SerializableSupplier[B] {
      def a = CoderUtils.decodeFromByteArray(coder, encoded)
      def get() = builder(a)
    }
  }

  /**
   * This is equivalent to [[org.apache.beam.sdk.testing.PAssert#containsInAnyOrder()]] but will but
   * have a nicer message in case of failure.
   */
  def containsInAnyOrder[T: Coder](
    ts: Seq[T],
    context: ScioContext
  ): h.Matcher[JIterable[T]] =
    SerializableMatchers.fromSupplier {
      supplierFromCoder(ts, context) { ds =>
        val items = ds.mkString("\n\t\t", "\n\t\t", "\n")
        val message = s"Expected: iterable with items [$items]"
        val c = Matchers
          .containsInAnyOrder(ds: _*)
          .asInstanceOf[h.Matcher[JIterable[T]]]
        new h.BaseMatcher[JIterable[T]] {
          override def matches(o: AnyRef): Boolean = c.matches(o)
          override def describeTo(d: h.Description): Unit = d.appendText(message)
          override def describeMismatch(i: AnyRef, d: h.Description): Unit =
            c.describeMismatch(i, d)
        }
      }
    }

  def makeFn[T](
    f: JIterable[T] => Unit
  ): SerializableFunction[JIterable[T], Void] =
    new SerializableFunction[JIterable[T], Void] {
      // delegate serialization to Kryo to avoid serialization issues in tests
      // when a non-serializable object is captured by the closure
      private[this] val impl = Externalizer(f)

      override def apply(input: JIterable[T]): Void = {
        impl.get(input)
        null
      }
    }

  def makeFnSingle[T](f: T => Unit): SerializableFunction[T, Void] =
    new SerializableFunction[T, Void] {
      // delegate serialization to Kryo to avoid serialization issues in tests
      // when a non-serializable object is captured by the closure
      private[this] val impl = Externalizer(f)

      override def apply(input: T): Void = {
        impl.get(input)
        null
      }
    }

  def assertThatFn[T: Eq: Coder](
    mm: h.Matcher[JIterable[TestWrapper[T]]]
  ): SerializableFunction[JIterable[T], Void] =
    makeFn[T](in => assertThat(TestWrapper.wrap(in), mm))

  def assertThatNotFn[T: Eq: Coder](
    mm: h.Matcher[JIterable[TestWrapper[T]]]
  ): SerializableFunction[JIterable[T], Void] =
    makeFn[T](in => assertThat(TestWrapper.wrap(in), Matchers.not(mm)))

  def assert[T: Eq: Coder](
    p: Iterable[TestWrapper[T]] => Boolean
  ): SerializableFunction[JIterable[T], Void] =
    makeFn[T](in => Predef.assert(p(TestWrapper.wrap(in).asScala)))

  def assertSingle[T: Eq: Coder](p: TestWrapper[T] => Boolean): SerializableFunction[T, Void] =
    makeFnSingle[T](in => Predef.assert(p(TestWrapper(in))))

  def assertNot[T: Eq: Coder](
    p: Iterable[TestWrapper[T]] => Boolean
  ): SerializableFunction[JIterable[T], Void] =
    makeFn[T](in => Predef.assert(!p(TestWrapper.wrap(in).asScala)))

  def assertNotSingle[T: Eq: Coder](p: TestWrapper[T] => Boolean): SerializableFunction[T, Void] =
    makeFnSingle[T](in => Predef.assert(!p(TestWrapper(in))))

  def isEqualTo[T: Eq: Coder](context: ScioContext, t: T): SerializableFunction[T, Void] = {
    val mm: SerializableMatcher[Any] =
      SerializableMatchers.fromSupplier {
        supplierFromCoder(t, context)(t => Matchers.equalTo[Any](TestWrapper(t)))
      }
    makeFnSingle[T](in => assertThat(TestWrapper(in), mm))
  }

  def notEqualTo[T: Eq: Coder](context: ScioContext, t: T): SerializableFunction[T, Void] = {
    val mm: SerializableMatcher[Any] =
      SerializableMatchers.fromSupplier {
        supplierFromCoder(t, context)(t => Matchers.not(Matchers.equalTo[Any](TestWrapper(t))))
      }

    makeFnSingle[T](in => assertThat(TestWrapper(in), mm))
  }

  def hasItem[T: Coder](t: T, context: ScioContext): h.Matcher[JIterable[T]] =
    SerializableMatchers.fromSupplier {
      supplierFromCoder(t, context) { t =>
        Matchers.hasItem(t).asInstanceOf[h.Matcher[JIterable[T]]]
      }
    }
}

/**
 * Trait with ScalaTest [[org.scalatest.matchers.Matcher Matcher]] s for
 * [[com.spotify.scio.values.SCollection SCollection]] s.
 */
trait SCollectionMatchers extends EqInstances {

  import ScioMatchers.makeFn

  sealed trait MatcherBuilder[T] {
    _: Matcher[T] =>

    type From
    type To >: From
    type AssertBuilder = From => To

    def matcher(builder: AssertBuilder): Matcher[T]

    def matcher: Matcher[T] = matcher(identity)
  }

  sealed trait IterableMatcher[T, B] extends MatcherBuilder[T] with Matcher[T] {
    type From = IterableAssert[B]
    type To = From

    override def apply(left: T): MatchResult = matcher(left)
  }

  sealed trait SingleMatcher[T, B] extends MatcherBuilder[T] with Matcher[T] {
    type From = SingletonAssert[B]
    type To = From

    override def apply(left: T): MatchResult = matcher(left)
  }

  /*
  Wrapper for PAssert statements. PAssert does not perform assertions or throw exceptions until
  sc.run() is called. So MatchResult should always match true for "a should $Matcher" cases and
  false for "a shouldNot $Matcher" cases. We also need to run different assertions for positive
  (shouldFn) and negative (shouldNotFn) cases.
   */
  private def m(shouldFn: () => Any, shouldNotFn: () => Any): MatchResult = {
    val isShouldNot = Thread
      .currentThread()
      .getStackTrace
      .filter(_.getClassName.startsWith("org.scalatest."))
      .exists(e => e.getClassName.contains("NotWord") || e.getMethodName == "shouldNot")
    val r = if (isShouldNot) {
      shouldNotFn()
      false
    } else {
      shouldFn()
      true
    }
    MatchResult(r, "", "")
  }

  // Due to  https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues/434
  // SerDe cycle on each element to keep consistent with values on the expected side
  private def serDeCycle[T: Coder](scollection: SCollection[T]): SCollection[T] = {
    val coder = scollection.internal.getCoder
    scollection
      .map(e =>
        CoderUtils
          .decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, e))
      )
  }

  /**
   * SCollection assertion only applied to the specified window, running the checker only on the
   * on-time pane for each key.
   */
  def inOnTimePane[T: ClassTag](window: BoundedWindow)(matcher: MatcherBuilder[T]): Matcher[T] =
    matcher match {
      case value: SingleMatcher[T, _] =>
        value.matcher(_.inOnTimePane(window))
      case value: IterableMatcher[T, _] =>
        value.matcher(_.inOnTimePane(window))
    }

  /** SCollection assertion only applied to the specified window. */
  def inWindow[T: ClassTag, B: ClassTag](
    window: BoundedWindow
  )(matcher: IterableMatcher[T, B]): Matcher[T] =
    matcher.matcher(_.inWindow(window))

  /**
   * SCollection assertion only applied to the specified window across all panes that were not
   * produced by the arrival of late data.
   */
  def inCombinedNonLatePanes[T: ClassTag, B: ClassTag](
    window: BoundedWindow
  )(matcher: IterableMatcher[T, B]): Matcher[T] =
    matcher.matcher(_.inCombinedNonLatePanes(window))

  /**
   * SCollection assertion only applied to the specified window, running the checker only on the
   * final pane for each key.
   */
  def inFinalPane[T: ClassTag, B: ClassTag](
    window: BoundedWindow
  )(matcher: MatcherBuilder[T]): Matcher[T] =
    matcher match {
      case value: SingleMatcher[T, _] =>
        value.matcher(_.inFinalPane(window))
      case value: IterableMatcher[T, _] =>
        value.matcher(_.inFinalPane(window))
    }

  /**
   * SCollection assertion only applied to the specified window, running the checker only on the
   * late pane for each key.
   */
  def inLatePane[T: ClassTag, B: ClassTag](
    window: BoundedWindow
  )(matcher: MatcherBuilder[T]): Matcher[T] =
    matcher match {
      case value: SingleMatcher[T, _] =>
        value.matcher(_.inLatePane(window))
      case value: IterableMatcher[T, _] =>
        value.matcher(_.inLatePane(window))
    }

  /**
   * SCollection assertion only applied to the specified window, running the checker only on the
   * early pane for each key.
   */
  def inEarlyPane[T](
    window: BoundedWindow
  )(matcher: MatcherBuilder[T]): Matcher[T] =
    matcher match {
      case value: SingleMatcher[T, _] =>
        value.matcher(_.inEarlyPane(window))
      case value: IterableMatcher[T, _] =>
        value.matcher(_.inEarlyPane(window))
    }

  /**
   * SCollection assertion only applied to the specified window. The assertion expect outputs to be
   * produced to the provided window exactly once.
   */
  def inOnlyPane[T: ClassTag, B: ClassTag](
    window: BoundedWindow
  )(matcher: SingleMatcher[T, B]): Matcher[T] =
    matcher.matcher(_.inOnlyPane(window))

  /** SCollection assertion only applied to early timing global window. */
  def inEarlyGlobalWindowPanes[T: ClassTag, B: ClassTag](
    matcher: IterableMatcher[T, B]
  ): Matcher[T] =
    matcher.matcher(_.inEarlyGlobalWindowPanes)

  /** Assert that the SCollection in question contains the provided elements. */
  def containInAnyOrder[T: Coder: Eq](
    value: Iterable[T]
  ): IterableMatcher[SCollection[T], T] =
    new IterableMatcher[SCollection[T], T] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[T]] =
        new Matcher[SCollection[T]] {
          override def apply(left: SCollection[T]): MatchResult = {
            val v = Externalizer(value.toSeq.map(x => TestWrapper(x))) // defeat closure
            val mm = ScioMatchers.containsInAnyOrder(v.get, left.context)
            val f = ScioMatchers.assertThatNotFn[T](mm)
            val g = ScioMatchers.assertThatFn[T](mm)
            val assertion = builder(PAssert.that(serDeCycle(left).internal))
            m(
              () => assertion.satisfies(g),
              () => assertion.satisfies(f)
            )
          }
        }
    }

  /** Assert that the SCollection in question contains a single provided element. */
  def containSingleValue[T: Coder: Eq](value: T): SingleMatcher[SCollection[T], T] =
    new SingleMatcher[SCollection[T], T] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[T]] =
        new Matcher[SCollection[T]] {
          override def apply(left: SCollection[T]): MatchResult = {
            ScioMatchers
            val assertion = builder(PAssert.thatSingleton(serDeCycle(left).internal))
            m(
              () => assertion.satisfies(ScioMatchers.isEqualTo(left.context, value)),
              () => assertion.satisfies(ScioMatchers.notEqualTo(left.context, value))
            )
          }
        }
    }

  /**
   * Assert that the SCollection in question contains the provided element without making
   * assumptions about other elements in the collection.
   */
  def containValue[T: Coder: Eq](value: T): IterableMatcher[SCollection[T], T] =
    new IterableMatcher[SCollection[T], T] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[T]] =
        new Matcher[SCollection[T]] {
          override def apply(left: SCollection[T]): MatchResult = {
            val v = Externalizer(TestWrapper[T](value)) // defeat closure
            val mm = ScioMatchers.hasItem(v.get, left.context)
            val should = ScioMatchers.assertThatFn[T](mm)
            val shouldNot = ScioMatchers.assertThatNotFn[T](mm)
            val assertion = builder(PAssert.that(serDeCycle(left).internal))
            m(
              () => assertion.satisfies(should),
              () => assertion.satisfies(shouldNot)
            )
          }
        }
    }

  /** Assert that the SCollection in question is empty. */
  val beEmpty: IterableMatcher[SCollection[_], Any] =
    new IterableMatcher[SCollection[_], Any] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[_]] =
        new Matcher[SCollection[_]] {
          override def apply(left: SCollection[_]): MatchResult = {
            val assertion = PAssert.that(left.asInstanceOf[SCollection[Any]].internal)
            m(
              () => builder(assertion).empty(),
              () =>
                builder(assertion)
                  .satisfies(makeFn(in => assert(in.iterator().hasNext, "SCollection is empty")))
            )
          }
        }
    }

  /** Assert that the SCollection in question has provided size. */
  def haveSize(size: Int): IterableMatcher[SCollection[_], Any] =
    new IterableMatcher[SCollection[_], Any] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[_]] =
        new Matcher[SCollection[_]] {
          override def apply(left: SCollection[_]): MatchResult = {
            val s = size
            val f = makeFn[Any] { in =>
              val inSize = in.asScala.size
              assert(inSize == s, s"SCollection expected size: $s, actual: $inSize")
            }
            val g = makeFn[Any] { in =>
              val inSize = in.asScala.size
              assert(inSize != s, s"SCollection expected size: not $s, actual: $inSize")
            }
            val assertion = PAssert.that(left.asInstanceOf[SCollection[Any]].internal)
            m(
              () => builder(assertion).satisfies(f),
              () => builder(assertion).satisfies(g)
            )
          }
        }
    }

  /** Assert that the SCollection in question is equivalent to the provided map. */
  def equalMapOf[K: Coder, V: Coder](
    value: Map[K, V]
  ): SingleMatcher[SCollection[(K, V)], JMap[K, V]] =
    new SingleMatcher[SCollection[(K, V)], JMap[K, V]] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[(K, V)]] =
        new Matcher[SCollection[(K, V)]] {
          override def apply(left: SCollection[(K, V)]): MatchResult = {
            val assertion = builder(PAssert.thatMap(serDeCycle(left).toKV.internal))
            m(
              () => assertion.isEqualTo(value.asJava),
              () => assertion.notEqualTo(value.asJava)
            )
          }
        }
    }

  // TODO: investigate why multi-map doesn't work

  /** Assert that the SCollection in question satisfies the provided function. */
  def satisfy[T: Coder: Eq](
    predicate: Iterable[T] => Boolean
  ): IterableMatcher[SCollection[T], T] =
    new IterableMatcher[SCollection[T], T] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[T]] =
        new Matcher[SCollection[T]] {
          override def apply(left: SCollection[T]): MatchResult = {
            val cleanedPredicate = ClosureCleaner.clean(predicate)
            val p: Iterable[TestWrapper[T]] => Boolean = it => cleanedPredicate(it.map(_.get))
            val f = ScioMatchers.assert(p)
            val g = ScioMatchers.assertNot(p)
            val assertion = builder(PAssert.that(serDeCycle(left).internal))
            m(
              () => assertion.satisfies(f),
              () => assertion.satisfies(g)
            )
          }
        }
    }

  /**
   * Assert that the SCollection in question contains a single element which satisfies the provided
   * function.
   */
  def satisfySingleValue[T: Coder: Eq](
    predicate: T => Boolean
  ): SingleMatcher[SCollection[T], T] =
    new SingleMatcher[SCollection[T], T] {
      override def matcher(builder: AssertBuilder): Matcher[SCollection[T]] =
        new Matcher[SCollection[T]] {
          override def apply(left: SCollection[T]): MatchResult = {
            val cleanedPredicate = ClosureCleaner.clean(predicate)
            val p: TestWrapper[T] => Boolean = t => cleanedPredicate(t.get)
            val f = ScioMatchers.assertSingle(p)
            val g = ScioMatchers.assertNotSingle(p)
            val assertion = builder(PAssert.thatSingleton(serDeCycle(left).internal))
            m(
              () => assertion.satisfies(f),
              () => assertion.satisfies(g)
            )
          }
        }
    }

  /** Assert that all elements of the SCollection in question satisfy the provided function. */
  def forAll[T: Coder: Eq](
    predicate: T => Boolean
  ): IterableMatcher[SCollection[T], T] = {
    val f = ClosureCleaner.clean(predicate)
    satisfy(_.forall(f))
  }

  /** Assert that some elements of the SCollection in question satisfy the provided function. */
  def exist[T: Coder: Eq](
    predicate: T => Boolean
  ): IterableMatcher[SCollection[T], T] = {
    val f = ClosureCleaner.clean(predicate)
    satisfy(_.exists(f))
  }
}
