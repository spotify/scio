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

package com.spotify.scio.algebra.syntax

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import cats.data.Nested
import cats.kernel.{CommutativeMonoid, Order}
import cats.{Applicative, FlatMap, Foldable, Functor, FunctorFilter, Traverse}
import com.spotify.scio.coders._
import com.spotify.scio.util.Functions._
import com.spotify.scio.values._
import org.apache.beam.sdk.transforms.Combine

/**
 * <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>
 *
 * [[SCollection]] functions that operate over types.
 */
final class AlgebraOps[A](private val coll: SCollection[A]) extends AnyVal {

  /**
   * Fold implemented using the given CommutativeMonoid[A] instance.
   */
  def fold_(implicit M: CommutativeMonoid[A], C: Coder[A]): SCollection[A] =
    coll.context.wrap {
      val combine = Combine.globally(new ReduceFn[A] {
        override val vacoder: Coder[JList[A]] = Coder[JList[A]]
        override val vocoder: Coder[A] = C

        override def reduceOption(accumulator: JIterable[A]): Option[A] =
          Fns
            .reduceOption(accumulator) { (a, b) =>
              M.combine(a, b)
            }
            .orElse(Some(M.empty))

        override def defaultValue(): A = M.empty

        override val context: CombineContext = CombineContext(coll.context)
      })

      coll.applyInternal(combine)
    }

  /**
   * Return the min as defined by the implicit `Order[T]`.
   */
  def min_(implicit O: Order[A], C: Coder[A]): SCollection[A] =
    coll.context.wrap {
      val combine = Combine.globally(new ReduceFn[A] {
        override val vacoder: Coder[JList[A]] = Coder[JList[A]]
        override val vocoder: Coder[A] = C

        override def reduceOption(accumulator: JIterable[A]): Option[A] =
          Fns.reduceOption(accumulator)(O.min)

        override val context: CombineContext = CombineContext(coll.context)
      })

      coll.applyInternal(combine)
    }

  /**
   * Return the max as defined by the implicit `Order[T]`.
   */
  def max_(implicit O: Order[A], C: Coder[A]): SCollection[A] =
    coll.context.wrap {
      val combine = Combine.globally(new ReduceFn[A] {
        override val vacoder: Coder[JList[A]] = Coder[JList[A]]
        override val vocoder: Coder[A] = C

        override def reduceOption(accumulator: JIterable[A]): Option[A] =
          Fns.reduceOption(accumulator)(O.max)

        override val context: CombineContext = CombineContext(coll.context)
      })

      coll.applyInternal(combine)
    }

}

/**
 * <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>
 *
 * [[SCollection]] functions that operate over F[_] types.
 */
final class HigherKindAlgebraOps[F[_], A](private val coll: SCollection[F[A]]) extends AnyVal {

  /**
   * Replaces the `A` value in `F[A]` with the supplied value.
   *
   * Example:
   *
   * {{{
   * scala> val coll: SCollection[List[Int]] = sc.paralelize(Seq(List(1, 2, 3)))
   * scala> coll.as("hello")
   * res0: SCollection[List[String]](List(hello, hello, hello))
   * }}}
   */
  def as[B](b: B)(implicit F: Functor[F], coder: Coder[F[B]]): SCollection[F[B]] =
    coll.map(F.as(_, b))

  /**
   * Tuples the `A` value in `F[A]` with the supplied `B` value, with the `B` value on the left.
   *
   * Example:
   * {{{
   * scala> val coll: SCollection[List[String]] = sc.paralelize(Seq(List("hello", "world")))
   * scala> coll.tupleLeft(42)
   * res0: SCollection[List[(Int, String)]](List((42,hello), (42,world)))
   * }}}
   */
  def tupleLeft[B](b: B)(implicit F: Functor[F], coder: Coder[F[(B, A)]]): SCollection[F[(B, A)]] =
    coll.map(F.tupleLeft(_, b))

  /**
   * Tuples the `A` value in `F[A]` with the supplied `B` value, with the `B` value on the right.
   *
   * Example:
   * Example:
   * {{{
   * scala> val coll: SCollection[List[String]] = sc.paralelize(Seq(List("hello", "world")))
   * scala> coll.tupleRight(42)
   * res0: SCollection[List[(String, Int)]](List((hello, 42), (world, 42)))
   * }}}
   */
  def tupleRight[B](b: B)(implicit F: Functor[F], coder: Coder[F[(A, B)]]): SCollection[F[(A, B)]] =
    coll.map(F.tupleRight(_, b))

  /**
   * Apply function to `F[A]`
   *
   * {{{
   * scala> val coll: SCollection[Map[Int, String]] =
   *          sc.paralelize(Seq(Map(1 -> "hi", 2 -> "there", 3 -> "you")))
   * scala> coll.mapF(_ ++ "!")
   * res0: SCollection[Map[Int, String]](Map(1 -> hi!, 2 -> there!, 3 -> you!))
   * }}}
   */
  def mapF[B](f: A => B)(implicit F: Functor[F], coder: Coder[F[B]]): SCollection[F[B]] =
    coll.map(F.map(_)(f))

  /**
   * Allows us to have a value in a context (F[A]) and then feed that into a function that takes
   * a normal value and returns a value in a context (A => F[B]).
   */
  def flatMapF[B](f: A => F[B])(implicit F: FlatMap[F], coder: Coder[F[B]]): SCollection[F[B]] =
    coll.map(F.flatMap(_)(f))

  /**
   * Tuple the values in fa with the result of applying a function
   * with the value
   *
   * Example:
   * {{{
   * scala> val coll: SCollection[Option[Int]] = sc.paralelize(Seq(Option(42)))
   * scala> coll.productF(_.toString)
   * res0: SCollection[Option[(Int, String)]](Some((42, 42)))
   * }}}
   */
  def productF[B](
    f: A => B
  )(implicit F: Functor[F], coder: Coder[F[(A, B)]]): SCollection[F[(A, B)]] =
    coll.map(F.fproduct(_)(f))

  /**
   * Pair `A` with the result of function application.
   *
   * Example:
   * {{{
   * scala> val coll: SCollection[List[String]] = sc.paralelize(Seq(List("12", "34", "56")))
   * scala> coll.mproductF(_.toList)
   * res0: SCollection[List[(String, String)]](List((12,1), (12,2), (34,3), (34,4), (56,5), (56,6)))
   * }}}
   */
  def mproductF[B](
    f: A => F[B]
  )(implicit F: FlatMap[F], coder: Coder[F[(A, B)]]): SCollection[F[(A, B)]] =
    coll.map(F.mproduct(_)(f))

  /**
   * A combined `map` and `filter`. Filtering is handled via `Option`
   * instead of `Boolean` such that the output type `B` can be different than
   * the input type `A`.
   *
   * Example:
   * {{{
   * scala> val m: Map[Int, String] = Map(1 -> "one", 3 -> "three")
   * scala> val data = Seq(Option(1), Option(2), None)
   * scala> sc.parallelize(data).mapFilterF(m.get(_))
   * res0: SCollection[Option[String]](Some("one"), None, None)
   *
   * scala> val listData = Seq(List(1, 2, 3), List(4, 5, 6))
   * scala> sc.parallelize(listData).mapFilterF(m.get(_))
   * res0: SCollection[List[String]](List("one", "three"), List())
   * }}}
   */
  def mapFilterF[B](
    f: A => Option[B]
  )(implicit F: FunctorFilter[F], coder: Coder[F[B]]): SCollection[F[B]] =
    coll.map(F.mapFilter(_)(f))

  def collectF[B](
    f: PartialFunction[A, B]
  )(implicit F: FunctorFilter[F], coder: Coder[F[B]]): SCollection[F[B]] =
    coll.map(F.collect(_)(f))

  /**
   * "flatten" a nested structure into a single-layer `A` layer.
   *
   * Example:
   * {{{
   * scala> val coll: SCollection[List[Int]] = sc.paralelize(Seq(List(1, 2, 3)))
   * scala> coll.flattenF
   * res0: SCollection[Int](1, 2, 3)
   * }}}
   */
  def flatten_(implicit FF: Foldable[F], c: Coder[A]): SCollection[A] =
    coll.transform(_.map(FF.toList).flatMap(identity))

  /**
   * Apply a filter to a structure such that the output structure contains all
   * `A` elements in the input structure that satisfy the predicate `f` but none
   * that don't.
   *
   * Example:
   * {{{
   * scala> val data = Seq(Option(1), Option(2), None)
   * scala> sc.parallelize(data).filterF(_ <= 1)
   * res0: SCollection[Option[Int]](Option(1), None, None)
   *
   * scala> val listData = Seq(List(1, 2, 3), List(4, 5, 6))
   * scala> sc.parallelize(listData).filterF(_ <= 1)
   * res0: SCollection[List[Int]](List(1), List())
   * }}}
   */
  def filterF(p: A => Boolean)(implicit F: FunctorFilter[F], c: Coder[F[A]]): SCollection[F[A]] =
    coll.map(F.filter(_)(p))

  /**
   * Similar to [[filterF]] but filters all non empty `F[A]` that satisfy the predicate.
   *
   * Example:
   * {{{
   * scala> val coll: SCollection[List[Int]] = sc.paralelize(Seq(List(1, 2, 3), List(-1, 0)))
   * scala> coll.nonEmptyF(_ > 1)
   * res0: SCollection[List[Int]](List(2, 3))
   * }}}
   */
  def nonEmptyF(
    p: A => Boolean
  )(implicit F: FunctorFilter[F], FF: Foldable[F], c: Coder[F[A]]): SCollection[F[A]] =
    coll.transform(_.filterF(p).nonEmptyF)

  /**
   * Filters all non empty `F[A]`
   */
  def nonEmptyF(implicit FF: Foldable[F], c: Coder[F[A]]): SCollection[F[A]] =
    coll.filter(FF.nonEmpty)

  /**
   * Similar to [[filterF]] but filters all empty `F[A]` that satisfy the predicate.
   *
   * Example:
   * {{{
   * scala> val coll: SCollection[List[Int]] = sc.paralelize(Seq(List(1, 2, 3), List(-1, 0)))
   * scala> coll.emptyF(_ > 1)
   * }}}
   */
  def emptyF(
    p: A => Boolean
  )(implicit F: FunctorFilter[F], FF: Foldable[F], c: Coder[F[A]]): SCollection[F[A]] =
    coll.transform(_.filterF(p).emptyF)

  /**
   * Filters all empty `F[A]`
   */
  def emptyF(implicit FF: Foldable[F], c: Coder[F[A]]): SCollection[F[A]] =
    coll.filter(FF.isEmpty)

  /**
   * Filter the elements that satisfy a predicate.
   */
  def filter_(
    p: A => Boolean
  )(implicit F: FunctorFilter[F], FF: Foldable[F], c: Coder[F[A]]): SCollection[F[A]] =
    coll.nonEmptyF(p)

  /**
   * Fold implemented using the given Monoid[A] instance.
   */
  def foldF(implicit F: Foldable[F], M: CommutativeMonoid[A], c: Coder[A]): SCollection[A] =
    coll.map(F.fold(_))

  def minOptionF(
    implicit F: Foldable[F],
    o: Order[A],
    c: Coder[Option[A]]
  ): SCollection[Option[A]] =
    coll.map(F.minimumOption(_))

  def maxOptionF(
    implicit F: Foldable[F],
    o: Order[A],
    c: Coder[Option[A]]
  ): SCollection[Option[A]] =
    coll.map(F.maximumOption(_))

  /**
   * Given a function which returns a G effect, thread this effect
   * through the running of this function on all the values in F,
   * returning an F[B] in a G context.
   *
   * Example:
   * {{{
   * scala> val data = Seq(List(Some(1), Some(2), None), List(Some(1), Some(2), Some(3)))
   * scala> sc.parallelize(data).traverse(identity)
   * res0: SCollection[Option[List[Int]]](None, Some(List(1, 2, 3)))
   * }}}
   */
  def traverse[G[_]: Applicative, B](
    f: A => G[B]
  )(implicit T: Traverse[F], coder: Coder[G[F[B]]]): SCollection[G[F[B]]] =
    coll.map(T.traverse(_)(f))

  /**
   * A traverse followed by flattening the inner result.
   *
   * Example:
   * {{{
   * scala> val data = Seq(Option(List("1", "2", "3", "four")))
   * scala> val parseInt: String => Option[Int] = s => Try(s.toInt).toOption
   * scala> sc.parallelize(data).flatTraverse(_.map(parseInt))
   * res0: SCollection[List[Option[Int]]](List(Some(1), Some(2), Some(3), None))
   * }}}
   */
  def flatTraverse[G[_], B](
    f: A => G[F[B]]
  )(
    implicit T: Traverse[F],
    F: FlatMap[F],
    G: Applicative[G],
    coder: Coder[G[F[B]]]
  ): SCollection[G[F[B]]] =
    coll.map(T.flatTraverse(_)(f))

}

/**
 * <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>
 *
 * [[SCollection]] functions that operate over nested F[_] types.
 */
final class HigherKindNestedAlgebraOps[F[_], G[_], A](private val coll: SCollection[F[G[A]]])
    extends AnyVal {

  def mapF[B](f: A => B)(
    implicit F: Functor[F],
    G: Functor[G],
    N: Functor[Nested[F, G, ?]],
    coder: Coder[F[G[B]]]
  ): SCollection[F[G[B]]] =
    coll.map(n => N.map(Nested(n))(f).value)

  /**
   * Thread all the G effects through the F structure to invert the
   * structure from `F[G[A]]` to `G[F[A]]`.
   *
   * Example:
   * {{{
   * scala> val data = Seq(List(Option(1), Option(2)))
   * scala> sc.parallelize(data).sequence
   * res0: SCollection[Option[List[Int]]](Some(List(1, 2)))
   * }}}
   */
  def sequence(
    implicit T: Traverse[F],
    G: Applicative[G],
    coder: Coder[G[F[A]]]
  ): SCollection[G[F[A]]] =
    coll.traverse(identity)

}

trait SCollectionSyntax {
  implicit def algebraOps[A](s: SCollection[A]): AlgebraOps[A] =
    new AlgebraOps[A](s)

  implicit def higherKindedAlgebraOps[F[_], A](s: SCollection[F[A]]): HigherKindAlgebraOps[F, A] =
    new HigherKindAlgebraOps[F, A](s)

  implicit def higherKindedNestedAlgebraOps[F[_], G[_], A](
    s: SCollection[F[G[A]]]
  ): HigherKindNestedAlgebraOps[F, G, A] = new HigherKindNestedAlgebraOps[F, G, A](s)
}
