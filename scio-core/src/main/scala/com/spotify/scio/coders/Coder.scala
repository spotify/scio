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

import java.io.{InputStream, OutputStream}
import com.spotify.scio.IsJavaBean
import com.spotify.scio.coders.instances._
import com.spotify.scio.transforms.BaseAsyncLookupDoFn
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder, CustomCoder, StructuredCoder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.apache.beam.sdk.values.KV

import java.util.{Collections, List => JList, Objects}
import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._
import scala.collection.compat._
import scala.collection.{mutable => m, BitSet, SortedSet}
import scala.reflect.ClassTag
import scala.util.Try
import java.util.UUID

@implicitNotFound(
  """
Cannot find an implicit Coder instance for type:

  >> ${T}

  This can happen for a few reasons, but the most common case is that a data
  member somewhere within this type doesn't have an implicit Coder instance in scope.

  Here are some debugging hints:
    - For Option types, ensure that a Coder instance is in scope for the non-Option version.
    - For List and Seq types, ensure that a Coder instance is in scope for a single element.
    - For generic methods, you may need to add an implicit parameter so that
        def foo[T](coll: SCollection[SomeClass], param: String): SCollection[T]

      may become:
        def foo[T](coll: SCollection[SomeClass],
                   param: String)(implicit c: Coder[T]): SCollection[T]
                                                ^
      Alternatively, you can use a context bound instead of an implicit parameter:
        def foo[T: Coder](coll: SCollection[SomeClass], param: String): SCollection[T]
                    ^
      read more here: https://spotify.github.io/scio/migrations/v0.7.0-Migration-Guide#add-missing-context-bounds

    - You can check that an instance exists for Coder in the REPL or in your code:
        scala> com.spotify.scio.coders.Coder[Foo]
      And find the missing instance and construct it as needed.
"""
)
sealed trait Coder[T] extends Serializable

final private[scio] case class Singleton[T] private[coders] (typeName: String, supply: () => T)
    extends Coder[T] {
  override def toString: String = s"Singleton[$typeName]"
}

// This should not be a case class. equality must be reference equality to detect recursive coders
final private[scio] class Ref[T] private (val typeName: String, c: => Coder[T]) extends Coder[T] {
  def value: Coder[T] = c
  override def toString: String = s"Ref[$typeName]"
}

private[scio] object Ref {
  def apply[T](t: String, c: => Coder[T]): Ref[T] = new Ref[T](t, c)
  def unapply[T](c: Ref[T]): Some[(String, Coder[T])] = Some((c.typeName, c.value))
}

final case class RawBeam[T] private[coders] (beam: BCoder[T]) extends Coder[T]
final case class Beam[T] private[coders] (beam: BCoder[T]) extends Coder[T]
final case class Fallback[T] private[coders] (ct: ClassTag[T]) extends Coder[T] {
  override def toString: String = s"Fallback[$ct]"
}
final case class CoderTransform[T, U] private[coders] (
  typeName: String,
  c: Coder[U],
  f: BCoder[U] => Coder[T]
) extends Coder[T] {
  override def toString: String = s"CoderTransform[$typeName]($c)"
}
final case class Transform[T, U] private[coders] (
  typeName: String,
  c: Coder[U],
  t: T => U,
  f: U => T
) extends Coder[T] {
  override def toString: String = s"Transform[$typeName]($c)"
}

final case class Disjunction[T, Id] private[coders] (
  typeName: String,
  idCoder: Coder[Id],
  coder: Map[Id, Coder[T]],
  id: T => Id
) extends Coder[T] {
  override def toString: String = {
    val body = coder.map { case (id, v) => s"$id -> $v" }.mkString(", ")
    s"Disjunction[$typeName]($body)"
  }
}

final case class Record[T] private[coders] (
  typeName: String,
  cs: Array[(String, Coder[Any])],
  construct: Seq[Any] => T,
  destruct: T => IndexedSeq[Any]
) extends Coder[T] {
  override def toString: String = {
    val body = cs.map { case (k, v) => s"($k, $v)" }.mkString(", ")
    s"Record[$typeName]($body)"
  }
}

// KV are special in beam and need to be serialized using an instance of KvCoder.
final case class KVCoder[K, V] private[scio] (koder: Coder[K], voder: Coder[V]) extends Coder[KV[K, V]]

///////////////////////////////////////////////////////////////////////////////
// Materialized beam coders
///////////////////////////////////////////////////////////////////////////////
final private class SingletonCoder[T](
  val typeName: String,
  supply: () => T
) extends CustomCoder[T] {
  private lazy val singleton = supply()

  override def toString: String = s"SingletonCoder[$typeName]"

  override def equals(obj: Any): Boolean = obj match {
    case that: SingletonCoder[_] => typeName == that.typeName
    case _                       => false
  }

  override def hashCode(): Int = typeName.hashCode

  override def encode(value: T, outStream: OutputStream): Unit = {}
  override def decode(inStream: InputStream): T = singleton
  override def verifyDeterministic(): Unit = {}
  override def consistentWithEquals(): Boolean = true
  override def isRegisterByteSizeObserverCheap(value: T): Boolean = true
  override def getEncodedElementByteSize(value: T): Long = 0
}

final private class DisjunctionCoder[T, Id](
  val typeName: String,
  val idCoder: BCoder[Id],
  val coders: Map[Id, BCoder[T]],
  id: T => Id
) extends CustomCoder[T] {

  override def toString: String = {
    val body = coders.map { case (id, coder) => s"$id -> $coder" }.mkString(", ")
    s"DisjunctionCoder[$typeName]($body)"
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: DisjunctionCoder[_, _] =>
      typeName == that.typeName && idCoder == that.idCoder && coders == that.coders
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(typeName, coders)

  def encode(value: T, os: OutputStream): Unit = {
    val i = id(value)
    idCoder.encode(i, os)
    coders(i).encode(value, os)
  }

  def decode(is: InputStream): T = {
    val i = idCoder.decode(is)
    coders(i).decode(is)
  }

  override def verifyDeterministic(): Unit = {
    var cause = Option.empty[NonDeterministicException]
    val reasons = List.newBuilder[String]
    coders.foreach { case (id, c) =>
      try {
        c.verifyDeterministic()
      } catch {
        case e: NonDeterministicException =>
          cause = Some(e)
          reasons += s"case $id is using non-deterministic $c"
      }
    }

    cause.foreach { e =>
      throw new NonDeterministicException(this, reasons.result().asJava, e)
    }
  }

  override def consistentWithEquals(): Boolean =
    coders.values.forall(_.consistentWithEquals())

  override def structuralValue(value: T): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      coders(id(value)).structuralValue(value)
    }
}

// Coder used internally specifically for Magnolia derived coders.
// It's technically possible to define Product coders only in terms of `Coder.transform`
// This is just faster
final private[scio] class RecordCoder[T](
  val typeName: String,
  val cs: IndexedSeq[(String, BCoder[Any])],
  construct: Seq[Any] => T,
  destruct: T => IndexedSeq[Any]
) extends CustomCoder[T] {

  override def toString: String = {
    val body = cs.map { case (l, c) => s"$l -> $c" }.mkString(", ")
    s"RecordCoder[$typeName]($body)"
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: RecordCoder[_] =>
      typeName == that.typeName && cs == that.cs
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(typeName, cs)

  @inline def onErrorMsg[A](msg: => String)(f: => A): A =
    try {
      f
    } catch {
      case e: Exception => throw CoderStackTrace.append(e, msg)
    }

  override def encode(value: T, os: OutputStream): Unit = {
    val vs = destruct(value)
    var idx = 0
    while (idx < cs.length) {
      val (l, c) = cs(idx)
      val v = vs(idx)
      onErrorMsg(
        s"Exception while trying to `encode` an instance of $typeName: Can't encode field $l value $v"
      ) {
        c.encode(v, os)
      }
      idx += 1
    }
  }

  override def decode(is: InputStream): T = {
    val vs = Array.ofDim[Any](cs.length)
    var idx = 0
    while (idx < cs.length) {
      val (l, c) = cs(idx)
      val v = onErrorMsg(
        s"Exception while trying to `decode` an instance of $typeName: Can't decode field $l"
      ) {
        c.decode(is)
      }
      vs.update(idx, v)
      idx += 1
    }
    construct(vs)
  }

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    var cause = Option.empty[NonDeterministicException]
    val reasons = List.newBuilder[String]
    cs.foreach { case (l, c) =>
      try {
        c.verifyDeterministic()
      } catch {
        case e: NonDeterministicException =>
          cause = Some(e)
          reasons += s"field $l is using non-deterministic $c"
      }
    }

    cause.foreach { e =>
      throw new NonDeterministicException(this, reasons.result().asJava, e)
    }
  }

  override def consistentWithEquals(): Boolean = cs.forall(_._2.consistentWithEquals())

  override def structuralValue(value: T): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      val svs = Array.ofDim[Any](cs.length)
      val vs = destruct(value)
      var idx = 0
      while (idx < cs.length) {
        val (l, c) = cs(idx)
        val v = vs(idx)
        val sv = onErrorMsg(
          s"Exception while trying compute `structuralValue` for field $l with value $v"
        ) {
          c.structuralValue(v)
        }
        svs.update(idx, sv)
        idx += 1
      }
      // return a scala Seq which defines proper equality for structuralValue comparison
      svs.toSeq
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: T): Boolean = {
    val vs = destruct(value)
    var isCheap = true
    var idx = 0
    while (isCheap && idx < cs.length) {
      val (_, c) = cs(idx)
      val v = vs(idx)
      isCheap = c.isRegisterByteSizeObserverCheap(v)
      idx += 1
    }
    isCheap
  }

  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit = {
    val vs = destruct(value)
    var idx = 0
    while (idx < cs.length) {
      val (_, c) = cs(idx)
      val v = vs(idx)
      c.registerByteSizeObserver(v, observer)
      idx += 1
    }
  }
}

final private[scio] class TransformCoder[T, U](
  val typeName: String,
  val bcoder: BCoder[U],
  to: T => U,
  from: U => T
) extends CustomCoder[T] {

  // save original functions hashCodes to be sure we uniquely identify TransformCoders after serialization
  // on deserialization, those won't be recomputed even if the function objects aren't equal
  private val toHash: Int = to.hashCode()
  private val fromHash: Int = from.hashCode()

  override def toString: String = s"TransformCoder[$typeName]($bcoder)"

  override def equals(obj: Any): Boolean = obj match {
    case that: TransformCoder[_, _] =>
      typeName == that.typeName && bcoder == that.bcoder && toHash == that.toHash && fromHash == that.fromHash
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(
    typeName,
    bcoder,
    toHash: java.lang.Integer,
    fromHash: java.lang.Integer
  )
  override def encode(value: T, os: OutputStream): Unit =
    bcoder.encode(to(value), os)

  override def encode(value: T, os: OutputStream, context: BCoder.Context): Unit =
    bcoder.encode(to(value), os, context)

  override def decode(is: InputStream): T =
    from(bcoder.decode(is))

  override def decode(is: InputStream, context: BCoder.Context): T =
    from(bcoder.decode(is, context))

  override def verifyDeterministic(): Unit =
    bcoder.verifyDeterministic()

  // Here we make the assumption that mapping functions are idempotent
  override def consistentWithEquals(): Boolean =
    bcoder.consistentWithEquals()

  override def structuralValue(value: T): AnyRef =
    bcoder.structuralValue(to(value))

  override def isRegisterByteSizeObserverCheap(value: T): Boolean =
    bcoder.isRegisterByteSizeObserverCheap(to(value))

  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit =
    bcoder.registerByteSizeObserver(to(value), observer)
}

sealed abstract private[scio] class WrappedCoder[T] extends StructuredCoder[T] {
  def bcoder: BCoder[T]

  override def getCoderArguments: JList[_ <: BCoder[_]] =
    Collections.singletonList(bcoder)

  override def encode(value: T, os: OutputStream): Unit =
    bcoder.encode(value, os)
  override def encode(value: T, os: OutputStream, context: BCoder.Context): Unit =
    bcoder.encode(value, os, context)
  override def decode(is: InputStream): T =
    bcoder.decode(is)
  override def decode(is: InputStream, context: BCoder.Context): T =
    bcoder.decode(is, context)
  override def verifyDeterministic(): Unit =
    bcoder.verifyDeterministic()
  override def consistentWithEquals(): Boolean =
    bcoder.consistentWithEquals()
  override def structuralValue(value: T): AnyRef =
    bcoder.structuralValue(value)
  override def isRegisterByteSizeObserverCheap(value: T): Boolean =
    bcoder.isRegisterByteSizeObserverCheap(value)
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit =
    bcoder.registerByteSizeObserver(value, observer)
}

final private[scio] class RefCoder[T](var bcoder: BCoder[T]) extends WrappedCoder[T] {
  def this() = this(null)
  override def toString: String = bcoder.toString
}

final private[scio] class LazyCoder[T](val typeName: String, bc: => BCoder[T])
    extends WrappedCoder[T] {

  override lazy val bcoder: BCoder[T] = bc

  override def toString: String = s"LazyCoder[$typeName]"

  // stop call stack and only compare on typeName
  override def equals(obj: Any): Boolean = obj match {
    case that: LazyCoder[_] => typeName == that.typeName
    case _                  => false
  }

  override def hashCode(): Int = typeName.hashCode

  // stop call stack and not interfere with other result
  override def verifyDeterministic(): Unit = {}

  // stop call stack and not interfere with other result
  override def consistentWithEquals(): Boolean = true

  // stop call stack and not interfere with other result
  override def isRegisterByteSizeObserverCheap(value: T): Boolean = true
}

// Contains the materialization stack trace to provide a helpful stacktrace if an exception happens
final private[scio] class MaterializedCoder[T](
  val bcoder: BCoder[T],
  materializationStackTrace: Array[StackTraceElement]
) extends WrappedCoder[T] {

  def this(bcoder: BCoder[T]) = this(bcoder, CoderStackTrace.prepare)

  override def toString: String = bcoder.toString

  @inline private def catching[A](a: => A) =
    try {
      a
    } catch {
      case ex: Throwable =>
        // prior to scio 0.8, a wrapped exception was thrown. It is no longer the case, as some
        // backends (e.g. Flink) use exceptions as a way to signal from the Coder to the layers
        // above here; we therefore must alter the type of exceptions passing through this block.
        throw CoderStackTrace.append(ex, materializationStackTrace)
    }

  override def encode(value: T, os: OutputStream): Unit =
    catching(super.encode(value, os))

  override def encode(value: T, os: OutputStream, context: BCoder.Context): Unit =
    catching(super.encode(value, os, context))

  override def decode(is: InputStream): T =
    catching(super.decode(is))

  override def decode(is: InputStream, context: BCoder.Context): T =
    catching(super.decode(is, context))
}

/**
 * Coder Grammar is used to explicitly specify Coder derivation for types used in pipelines.
 *
 * The CoderGrammar can be used as follows:
 *   - To find the Coder being implicitly derived by Scio. (Debugging)
 *     {{{
 *     def c: Coder[MyType] = Coder[MyType]
 *     }}}
 *
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
 *
 *   - To explicitly use kryo Coder use [[Coder.kryo]]
 */
sealed trait CoderGrammar {

  /** Create a ScioCoder from a Beam Coder */
  def raw[T](beam: BCoder[T]): Coder[T] =
    RawBeam(beam)
  def beam[T](beam: BCoder[T]): Coder[T] =
    Beam(beam)
  def kv[K, V](koder: Coder[K], voder: Coder[V]): Coder[KV[K, V]] =
    KVCoder(koder, voder)

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

  // Internal use only
  protected[coders] def explicitXmap[U, T](c: Coder[U])(f: U => T, t: T => U)(typeName: String): Coder[T] = 
    Transform(typeName, c, t, f)
}

object Coder
    extends CoderGrammar
    with CoderFallback
    with TupleCoders
    with AvroCoders
    with ProtobufCoders
    with AlgebirdCoders
    with GuavaCoders
    with JodaCoders
    with BeamTypeCoders
    with LowPriorityCoders
    with LowPriorityCoderDerivation
    {
  @inline final def apply[T](implicit c: Coder[T]): Coder[T] = c

  implicit val charCoder: Coder[Char] = ScalaCoders.charCoder
  implicit val byteCoder: Coder[Byte] = ScalaCoders.byteCoder
  implicit val stringCoder: Coder[String] = ScalaCoders.stringCoder
  implicit val shortCoder: Coder[Short] = ScalaCoders.shortCoder
  implicit val intCoder: Coder[Int] = ScalaCoders.intCoder
  implicit val longCoder: Coder[Long] = ScalaCoders.longCoder
  implicit val floatCoder: Coder[Float] = ScalaCoders.floatCoder
  implicit val doubleCoder: Coder[Double] = ScalaCoders.doubleCoder
  implicit val booleanCoder: Coder[Boolean] = ScalaCoders.booleanCoder
  implicit val unitCoder: Coder[Unit] = ScalaCoders.unitCoder
  implicit val nothingCoder: Coder[Nothing] = ScalaCoders.nothingCoder
  implicit val bigIntCoder: Coder[BigInt] = ScalaCoders.bigIntCoder
  implicit val bigDecimalCoder: Coder[BigDecimal] = ScalaCoders.bigDecimalCoder
  implicit def tryCoder[A: Coder]: Coder[Try[A]] = ScalaCoders.tryCoder
  implicit def eitherCoder[A: Coder, B: Coder]: Coder[Either[A, B]] = ScalaCoders.eitherCoder
  implicit def optionCoder[T: Coder, S[_] <: Option[_]]: Coder[S[T]] = ScalaCoders.optionCoder
  implicit val noneCoder: Coder[None.type] = ScalaCoders.noneCoder
  implicit val bitSetCoder: Coder[BitSet] = ScalaCoders.bitSetCoder
  implicit def seqCoder[T: Coder]: Coder[Seq[T]] = ScalaCoders.seqCoder
  implicit def iterableCoder[T: Coder]: Coder[Iterable[T]] = ScalaCoders.iterableCoder
  implicit def throwableCoder[T <: Throwable: ClassTag]: Coder[T] = ScalaCoders.throwableCoder
  implicit def listCoder[T: Coder]: Coder[List[T]] = ScalaCoders.listCoder
  implicit def iterableOnceCoder[T: Coder]: Coder[IterableOnce[T]] =
    ScalaCoders.iterableOnceCoder
  implicit def setCoder[T: Coder]: Coder[Set[T]] = ScalaCoders.setCoder
  implicit def mutableSetCoder[T: Coder]: Coder[m.Set[T]] = ScalaCoders.mutableSetCoder
  implicit def vectorCoder[T: Coder]: Coder[Vector[T]] = ScalaCoders.vectorCoder
  implicit def arrayBufferCoder[T: Coder]: Coder[m.ArrayBuffer[T]] = ScalaCoders.arrayBufferCoder
  implicit def bufferCoder[T: Coder]: Coder[m.Buffer[T]] = ScalaCoders.bufferCoder
  implicit def listBufferCoder[T: Coder]: Coder[m.ListBuffer[T]] = ScalaCoders.listBufferCoder
  implicit def arrayCoder[T: Coder: ClassTag]: Coder[Array[T]] = ScalaCoders.arrayCoder
  implicit val arrayByteCoder: Coder[Array[Byte]] = ScalaCoders.arrayByteCoder
  implicit def wrappedArrayCoder[T: Coder: ClassTag](implicit
    wrap: Array[T] => m.WrappedArray[T]
  ): Coder[m.WrappedArray[T]] = ScalaCoders.wrappedArrayCoder
  implicit def mutableMapCoder[K: Coder, V: Coder]: Coder[m.Map[K, V]] = ScalaCoders.mutableMapCoder
  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] = ScalaCoders.mapCoder
  implicit def sortedSetCoder[T: Coder: Ordering]: Coder[SortedSet[T]] = ScalaCoders.sortedSetCoder

  implicit val voidCoder: Coder[Void] = JavaCoders.voidCoder
  implicit val uuidCoder: Coder[UUID] = JavaCoders.uuidCoder
  implicit val uriCoder: Coder[java.net.URI] = JavaCoders.uriCoder
  implicit val pathCoder: Coder[java.nio.file.Path] = JavaCoders.pathCoder
  implicit def jIterableCoder[T: Coder]: Coder[java.lang.Iterable[T]] = JavaCoders.jIterableCoder
  implicit def jlistCoder[T: Coder]: Coder[JList[T]] = JavaCoders.jlistCoder
  implicit def jArrayListCoder[T: Coder]: Coder[java.util.ArrayList[T]] = JavaCoders.jArrayListCoder
  implicit def jMapCoder[K: Coder, V: Coder]: Coder[java.util.Map[K, V]] = JavaCoders.jMapCoder
  implicit def jTryCoder[A](implicit c: Coder[Try[A]]): Coder[BaseAsyncLookupDoFn.Try[A]] =
    JavaCoders.jTryCoder
  implicit val jBitSetCoder: Coder[java.util.BitSet] = JavaCoders.jBitSetCoder
  implicit val jShortCoder: Coder[java.lang.Short] = JavaCoders.jShortCoder
  implicit val jByteCoder: Coder[java.lang.Byte] = JavaCoders.jByteCoder
  implicit val jIntegerCoder: Coder[java.lang.Integer] = JavaCoders.jIntegerCoder
  implicit val jLongCoder: Coder[java.lang.Long] = JavaCoders.jLongCoder
  implicit val jFloatCoder: Coder[java.lang.Float] = JavaCoders.jFloatCoder
  implicit val jDoubleCoder: Coder[java.lang.Double] = JavaCoders.jDoubleCoder
  implicit val jBooleanCoder: Coder[java.lang.Boolean] = JavaCoders.jBooleanCoder
  implicit val jBigIntegerCoder: Coder[java.math.BigInteger] = JavaCoders.jBigIntegerCoder
  implicit val jBigDecimalCoder: Coder[java.math.BigDecimal] = JavaCoders.jBigDecimalCoder
  implicit val serializableCoder: Coder[Serializable] = Coder.kryo[Serializable]
  implicit val jInstantCoder: Coder[java.time.Instant] = JavaCoders.jInstantCoder
  implicit val jLocalDateCoder: Coder[java.time.LocalDate] = JavaCoders.jLocalDateCoder
  implicit val jLocalTimeCoder: Coder[java.time.LocalTime] = JavaCoders.jLocalTimeCoder
  implicit val jLocalDateTimeCoder: Coder[java.time.LocalDateTime] = JavaCoders.jLocalDateTimeCoder
  implicit val jDurationCoder: Coder[java.time.Duration] = JavaCoders.jDurationCoder
  implicit val jPeriodCoder: Coder[java.time.Period] = JavaCoders.jPeriodCoder
  implicit val jSqlTimestamp: Coder[java.sql.Timestamp] = JavaCoders.jSqlTimestamp
  implicit def coderJEnum[E <: java.lang.Enum[E]: ClassTag]: Coder[E] = JavaCoders.coderJEnum
}

trait LowPriorityCoders extends LowPriorityCoderDerivation {
  implicit def javaBeanCoder[T: IsJavaBean: ClassTag]: Coder[T] = JavaCoders.javaBeanCoder
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
      .take(10)

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
