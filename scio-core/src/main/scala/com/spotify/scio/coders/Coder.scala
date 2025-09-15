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

package com.spotify.scio.coders

import com.spotify.scio.{IsJavaBean, MagnoliaMacros}
import com.spotify.scio.coders.instances._
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.values.KV

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

@implicitNotFound(
  """
Cannot find an implicit Coder instance for type:

  >> ${T}

  This can happen for a few reasons, but the most common case is that a data
  member somewhere within this type doesn't have an implicit Coder instance in scope.

  Here are some hints:
    - For collections, ensure that a Coder instance is in scope for the element type.
    - For module specific types, you may need to explicitly import the coders, eg avro:
        import com.spotify.scio.avro._
    - For sealed traits and case classes, you can identify the missing member's coder:
        scala> com.spotify.scio.coders.Coder.gen[Foo]

          error: magnolia: could not find Coder.Typeclass for type Bar
            in parameter 'xxx' of product type Foo
    - For generic methods, you may need to add an implicit parameter so that:
        def foo[T](coll: SCollection[SomeClass], param: String): SCollection[T]

      may become:
        def foo[T](coll: SCollection[SomeClass],
                   param: String)(implicit c: Coder[T]): SCollection[T]
                                  ^
      Alternatively, you can use a context bound instead of an implicit parameter:
        def foo[T: Coder](coll: SCollection[SomeClass], param: String): SCollection[T]
                 ^
"""
)
sealed trait Coder[T] extends Serializable

sealed private[scio] trait TypeName {
  def typeName: String
}

final private[scio] case class Singleton[T] private (typeName: String, supply: () => T)
    extends Coder[T]
    with TypeName {
  override def toString: String = s"Singleton[$typeName]"
}

// This should not be a case class. equality must be reference equality to detect recursive coders
final private[scio] class Ref[T] private (val typeName: String, c: => Coder[T])
    extends Coder[T]
    with TypeName {
  def value: Coder[T] = c
  override def toString: String = s"Ref[$typeName]"
}

private[scio] object Ref {
  def apply[T](t: String, c: => Coder[T]): Ref[T] = new Ref[T](t, c)
  def unapply[T](c: Ref[T]): Some[(String, Coder[T])] = Some((c.typeName, c.value))
}

final case class RawBeam[T] private (beam: BCoder[T]) extends Coder[T]
final case class Beam[T] private (beam: BCoder[T]) extends Coder[T]
final case class Fallback[T] private (ct: ClassTag[T]) extends Coder[T] {
  override def toString: String = s"Fallback[$ct]"
}
final case class CoderTransform[T, U] private (
  typeName: String,
  c: Coder[U],
  f: BCoder[U] => Coder[T]
) extends Coder[T]
    with TypeName {
  override def toString: String = s"CoderTransform[$typeName]($c)"
}
final case class Transform[T, U] private (
  typeName: String,
  c: Coder[U],
  t: T => U,
  f: U => T
) extends Coder[T]
    with TypeName {
  override def toString: String = s"Transform[$typeName]($c)"
}

final case class Disjunction[T, Id] private (
  typeName: String,
  idCoder: Coder[Id],
  coder: Map[Id, Coder[T]],
  id: T => Id
) extends Coder[T]
    with TypeName {
  override def toString: String = {
    val body = coder.map { case (id, v) => s"$id -> $v" }.mkString(", ")
    s"Disjunction[$typeName]($body)"
  }
}

final case class Record[T] private (
  typeName: String,
  cs: Array[(String, Coder[Any])],
  construct: Seq[Any] => T,
  destruct: T => IndexedSeq[Any]
) extends Coder[T]
    with TypeName {
  override def toString: String = {
    val body = cs.map { case (k, v) => s"($k, $v)" }.mkString(", ")
    s"Record[$typeName]($body)"
  }
}

// KV are special in beam and need to be serialized using an instance of KvCoder.
final case class KVCoder[K, V] private (koder: Coder[K], voder: Coder[V]) extends Coder[KV[K, V]]

// GroupByKey aggregate are special because they can't be wrapped
final case class AggregateCoder[T] private (coder: Coder[T]) extends Coder[java.lang.Iterable[T]]

/**
 * Coder Grammar is used to explicitly specify Coder derivation for types used in pipelines.
 *
 * The CoderGrammar can be used as follows:
 *   - To find the Coder being implicitly derived by Scio. (Debugging)
 *     {{{
 *     def c: Coder[MyType] = Coder[MyType]
 *     }}}
 *   - To generate an implicit instance to be in scope for type T, use [[Coder.gen]]
 *     {{{
 *     implicit def coderT: Coder[T] = Coder.gen[T]
 *     }}}
 *
 * Note: Implicit Coders for all parameters of the constructor of type T should be in scope for
 * [[Coder.gen]] to be able to derive the Coder.
 *
 *   - To define a Coder of custom type, where the type can be mapped to some other type for which a
 *     Coder is known, use [[Coder.xmap]]
 *   - To explicitly use kryo Coder use [[Coder.kryo]]
 */
private[coders] trait CoderGrammar {

  /** Create a ScioCoder from a Beam Coder */
  def raw[T](beam: BCoder[T]): Coder[T] =
    RawBeam(beam)
  def beam[T](beam: BCoder[T]): Coder[T] =
    Beam(beam)
  def kv[K, V](koder: Coder[K], voder: Coder[V]): Coder[KV[K, V]] =
    KVCoder(koder, voder)
  def aggregate[T: Coder]: Coder[java.lang.Iterable[T]] =
    AggregateCoder(Coder[T])

  /**
   * Create an instance of Kryo Coder for a given Type.
   *
   * Eg: A kryo Coder for [[org.joda.time.Interval]] would look like:
   * {{{
   *     implicit def jiKryo: Coder[Interval] = Coder.kryo[Interval]
   * }}}
   */
  def kryo[T](implicit ct: ClassTag[T]): Coder[T] =
    Fallback[T](ct)

  private[scio] def singleton[T](typeName: String, constructor: () => T): Coder[T] =
    Singleton[T](typeName, constructor)

  def transform[U, T](c: Coder[U])(f: BCoder[U] => Coder[T])(implicit ct: ClassTag[T]): Coder[T] =
    CoderTransform(ct.runtimeClass.getName, c, f)

  private[scio] def ref[T](typeName: String)(value: => Coder[T]): Coder[T] =
    Ref(typeName, value)

  private[scio] def record[T](
    typeName: String,
    cs: Array[(String, Coder[Any])]
  )(
    construct: Seq[Any] => T,
    destruct: T => IndexedSeq[Any]
  ): Coder[T] =
    Record[T](typeName, cs, construct, destruct)

  def disjunction[T, Id: Coder](typeName: String, coder: Map[Id, Coder[T]])(id: T => Id): Coder[T] =
    Disjunction(typeName, Coder[Id], coder, id)

  /**
   * Given a Coder[A], create a Coder[B] by defining two functions A => B and B => A. The Coder[A]
   * can be resolved implicitly by calling Coder[A]
   *
   * Eg: Coder for [[org.joda.time.Interval]] can be defined by having the following implicit in
   * scope. Without this implicit in scope Coder derivation falls back to Kryo.
   * {{{
   *       implicit def jiCoder: Coder[Interval] =
   *         Coder.xmap(Coder[(Long, Long)])(t => new Interval(t._1, t._2),
   *             i => (i.getStartMillis, i.getEndMillis))
   * }}}
   * In the above example we implicitly derive Coder[(Long, Long)] and we define two functions, one
   * to convert a tuple (Long, Long) to Interval, and a second one to convert an Interval to a tuple
   * of (Long, Long)
   */
  def xmap[U, T](c: Coder[U])(f: U => T, t: T => U)(implicit ct: ClassTag[T]): Coder[T] =
    Transform(ct.runtimeClass.getName, c, t, f)
}

object Coder
    extends ScalaCoders
    with TupleCoders
    with JavaCoders
    with JodaCoders
    with BeamTypeCoders
    with ProtobufCoders
    with AlgebirdCoders
    with GuavaCoders
    with CoderDerivation
    with LowPriorityCoders {
  @inline final def apply[T](implicit c: Coder[T]): Coder[T] = c
}

trait LowPriorityCoders extends LowPriorityCoders1 { self: CoderDerivation with JavaBeanCoders =>
  implicit override def gen[T]: Coder[T] = macro MagnoliaMacros.genWithoutAnnotations[T]
}

trait LowPriorityCoders1 { self: JavaBeanCoders =>
  implicit override def javaBeanCoder[T: IsJavaBean: ClassTag]: Coder[T] = JavaCoders.javaBeanCoder
}

private[coders] object CoderStackTrace {

  val CoderStackElemMarker = new StackTraceElement(
    "### Coder materialization stack ###",
    "",
    "",
    0
  )

  def prepare: Array[StackTraceElement] =
    CoderStackElemMarker +: Thread
      .currentThread()
      .getStackTrace
      .dropWhile(!_.getClassName.contains(CoderMaterializer.getClass.getName))
      .take(15)

  def append[T <: Throwable](
    cause: T,
    additionalMessage: String
  ): T = append(cause, Some(additionalMessage), Array.empty)

  def append[T <: Throwable](
    cause: T,
    baseStack: Array[StackTraceElement]
  ): T = append(cause, None, baseStack)

  private def append[T <: Throwable](
    cause: T,
    additionalMessage: Option[String],
    baseStack: Array[StackTraceElement]
  ): T = {
    val messageItem = additionalMessage.map { msg =>
      new StackTraceElement(s"Due to $msg", "", "", 0)
    }

    val adjustedStack = messageItem ++ cause.getStackTrace ++ baseStack
    cause.setStackTrace(adjustedStack.toArray)
    cause
  }
}
