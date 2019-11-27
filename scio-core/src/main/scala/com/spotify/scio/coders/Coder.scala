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
import java.lang.{Iterable => JIterable}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.time.Instant

import com.spotify.scio.coders.instances._
import com.spotify.scio.transforms.BaseAsyncLookupDoFn
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{AtomicCoder, Coder => BCoder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.apache.beam.sdk.values.KV

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.collection.{BitSet, SortedSet, TraversableOnce, mutable => m}
import scala.reflect.ClassTag
import scala.util.Try

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

private[scio] final class Ref[T](val typeName: String, c: => Coder[T]) extends Coder[T] {
  def value: Coder[T] = c

  override def toString(): String = s"""Ref($typeName)"""
}

private[scio] object Ref {
  def apply[T](t: String, c: => Coder[T]): Ref[T] = new Ref[T](t, c)
  def unapply[T](c: Ref[T]): Option[(String, Coder[T])] = Option((c.typeName, c.value))
}

final case class Beam[T] private (beam: BCoder[T]) extends Coder[T] {
  override def toString: String = s"Beam($beam)"
}
final case class Fallback[T] private (ct: ClassTag[T]) extends Coder[T] {
  override def toString: String = s"Fallback($ct)"
}
final case class Transform[A, B] private (c: Coder[A], f: BCoder[A] => Coder[B]) extends Coder[B] {
  override def toString: String = s"Transform($c, $f)"
}
final case class Disjunction[T, Id] private (
  typeName: String,
  idCoder: Coder[Id],
  id: T => Id,
  coder: Map[Id, Coder[T]]
) extends Coder[T] {
  override def toString: String = s"Disjunction($typeName, $coder)"
}

final case class Record[T] private (
  typeName: String,
  cs: Array[(String, Coder[Any])],
  construct: Seq[Any] => T,
  destruct: T => Array[Any]
) extends Coder[T] {
  override def toString: String = {
    val str = cs
      .map {
        case (k, v) => s"($k, $v)"
      }
    s"Record($typeName, ${str.mkString(", ")})"
  }
}

// KV are special in beam and need to be serialized using an instance of KvCoder.
final case class KVCoder[K, V] private (koder: Coder[K], voder: Coder[V]) extends Coder[KV[K, V]] {
  override def toString: String = s"KVCoder($koder, $voder)"
}

private final case class DisjunctionCoder[T, Id](
  typeName: String,
  idCoder: BCoder[Id],
  id: T => Id,
  coders: Map[Id, BCoder[T]]
) extends AtomicCoder[T] {
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
    def verify(label: String, c: BCoder[_]): List[(String, NonDeterministicException)] = {
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"case $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    val problems =
      coders.toList.flatMap { case (id, c) => verify(id.toString, c) } ++
        verify("id", idCoder)

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def toString: String = {
    val parts = s"id -> $idCoder" :: coders.map { case (id, coder) => s"$id -> $coder" }.toList
    val body = parts.mkString(", ")

    s"DisjunctionCoder[$typeName]($body)"
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

private[scio] final case class RefCoder[T](val typeName: String, var coder: BCoder[T])
    extends BCoder[T] {
  def setImpl(_c: BCoder[T]): Unit = coder = _c

  private def check[A](t: => A): A = {
    require(coder != null, s"Coder implementation should not be null in ${this}")
    t
  }

  def decode(inStream: InputStream): T = check { coder.decode(inStream) }
  def encode(value: T, outStream: OutputStream): Unit = check { coder.encode(value, outStream) }
  def getCoderArguments(): java.util.List[_ <: BCoder[_]] = check { coder.getCoderArguments() }
  def verifyDeterministic(): Unit = check { coder.verifyDeterministic() }

  override def consistentWithEquals(): Boolean = check { coder.consistentWithEquals() }
  override def structuralValue(value: T): AnyRef = check {
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      coder.structuralValue(value)
    }
  }

  override def toString(): String =
    s"RefCoder($typeName, ${if (coder == null) "null" else "<coder ref>"})"
}

// XXX: Workaround a NPE deep down the stack in Beam
// info]   java.lang.NullPointerException: null value in entry: T=null
private[scio] case class WrappedBCoder[T](u: BCoder[T]) extends BCoder[T] {
  /**
   * Eagerly compute a stack trace on materialization
   * to provide a helpful stacktrace if an exception happens
   */
  private[this] val materializationStackTrace: Array[StackTraceElement] =
    CoderStackTrace.prepare

  override def toString: String = u.toString

  @inline private def catching[A](a: => A) =
    try {
      a
    } catch {
      case ex: Throwable =>
        // prior to scio 0.8, a wrapped exception was thrown. It is no longer the case, as some
        // backends (e.g. Flink) use exceptions as a way to signal from the Coder to the layers
        // above here; we therefore must alter the type of exceptions passing through this block.
        throw CoderStackTrace.append(ex, None, materializationStackTrace)
    }

  override def encode(value: T, os: OutputStream): Unit =
    catching { u.encode(value, os) }

  override def decode(is: InputStream): T =
    catching { u.decode(is) }

  override def getCoderArguments: java.util.List[_ <: BCoder[_]] = u.getCoderArguments

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit = u.verifyDeterministic()
  override def consistentWithEquals(): Boolean = u.consistentWithEquals()
  override def structuralValue(value: T): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      u.structuralValue(value)
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: T): Boolean =
    u.isRegisterByteSizeObserverCheap(value)
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit =
    u.registerByteSizeObserver(value, observer)
}

private[scio] object WrappedBCoder {
  def create[T](u: BCoder[T]): BCoder[T] =
    u match {
      case WrappedBCoder(_) => u
      case _                => new WrappedBCoder(u)
    }
}

// Coder used internally specifically for Magnolia derived coders.
// It's technically possible to define Product coders only in terms of `Coder.transform`
// This is just faster
private[scio] final case class RecordCoder[T](
  typeName: String,
  cs: Array[(String, BCoder[Any])],
  construct: Seq[Any] => T,
  destruct: T => Array[Any]
) extends AtomicCoder[T] {
  @inline def onErrorMsg[A](msg: => String)(f: => A): A =
    try {
      f
    } catch {
      case e: Exception =>
        throw new RuntimeException(msg, e)
    }

  override def encode(value: T, os: OutputStream): Unit = {
    var i = 0
    val array = destruct(value)
    while (i < array.length) {
      val (label, c) = cs(i)
      val v = array(i)
      onErrorMsg(
        s"Exception while trying to `encode` an instance of $typeName:  Can't encode field $label value $v"
      ) {
        c.encode(v, os)
      }
      i += 1
    }
  }

  override def decode(is: InputStream): T = {
    val vs = new Array[Any](cs.length)
    var i = 0
    while (i < cs.length) {
      val (label, c) = cs(i)
      onErrorMsg(
        s"Exception while trying to `decode` an instance of $typeName: Can't decode field $label"
      ) {
        vs.update(i, c.decode(is))
      }
      i += 1
    }
    construct(vs.toSeq)
  }

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val problems = cs.toList.flatMap {
      case (label, c) =>
        try {
          c.verifyDeterministic()
          Nil
        } catch {
          case e: NonDeterministicException =>
            val reason = s"field $label is using non-deterministic $c"
            List(reason -> e)
        }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def toString: String = {
    val body = cs.map { case (label, c) => s"$label -> $c" }.mkString(", ")
    s"RecordCoder[$typeName]($body)"
  }

  override def consistentWithEquals(): Boolean = cs.forall(_._2.consistentWithEquals())
  override def structuralValue(value: T): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      val b = Seq.newBuilder[AnyRef]
      var i = 0
      val array = destruct(value)
      while (i < cs.length) {
        val (label, c) = cs(i)
        val v = array(i)
        onErrorMsg(s"Exception while trying to `encode` field $label with value $v") {
          b += c.structuralValue(v)
        }
        i += 1
      }
      b.result()
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: T): Boolean = {
    var res = true
    var i = 0
    val array = destruct(value)
    while (res && i < cs.length) {
      res = cs(i)._2.isRegisterByteSizeObserverCheap(array(i))
      i += 1
    }
    res
  }
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit = {
    var i = 0
    val array = destruct(value)
    while (i < cs.length) {
      val (_, c) = cs(i)
      val v = array(i)
      c.registerByteSizeObserver(v, observer)
      i += 1
    }
  }
}

/**
 * Coder Grammar is used to explicitly specify Coder derivation for types used in pipelines.
 *
 * The CoderGrammar can be used as follows:
 * - To find the Coder being implicitly derived by Scio. (Debugging)
 *   {{{
 *     def c: Coder[MyType] = Coder[MyType]
 *   }}}
 *
 * - To generate an implicit instance to be in scope for type T, use [[Coder.gen]]
 *   {{{
 *     implicit def coderT: Coder[T] = Coder.gen[T]
 *   }}}
 *
 *   Note: Implicit Coders for all parameters of the constructor of type T should be in scope for
 *         [[Coder.gen]] to be able to derive the Coder.
 *
 * - To define a Coder of custom type, where the type can be mapped to some other type for which
 *   a Coder is known, use [[Coder.xmap]]
 *
 * - To explicitly use kryo Coder use [[Coder.kryo]]
 *
 */
sealed trait CoderGrammar {
  /**
   * Create a ScioCoder from a Beam Coder
   */
  def beam[T](beam: BCoder[T]): Coder[T] =
    Beam(beam)
  def kv[K, V](koder: Coder[K], voder: Coder[V]): Coder[KV[K, V]] =
    KVCoder(koder, voder)

  /**
   * Create an instance of Kryo Coder for a given Type.
   *
   * Eg:
   *   A kryo Coder for [[org.joda.time.Interval]] would look like:
   *   {{{
   *     implicit def jiKryo: Coder[Interval] = Coder.kryo[Interval]
   *   }}}
   */
  def kryo[T](implicit ct: ClassTag[T]): Coder[T] =
    Fallback[T](ct)
  def transform[A, B](c: Coder[A])(f: BCoder[A] => Coder[B]): Coder[B] =
    Transform(c, f)
  def disjunction[T, Id: Coder](typeName: String, coder: Map[Id, Coder[T]])(id: T => Id): Coder[T] =
    Disjunction(typeName, Coder[Id], id, coder)

  /**
   * Given a Coder[A], create a Coder[B] by defining two functions A => B and B => A.
   * The Coder[A] can be resolved implicitly by calling Coder[A]
   *
   * Eg: Coder for [[org.joda.time.Interval]] can be defined by having the following implicit in
   *     scope. Without this implicit in scope Coder derivation falls back to Kryo.
   *     {{{
   *       implicit def jiCoder: Coder[Interval] =
   *         Coder.xmap(Coder[(Long, Long)])(t => new Interval(t._1, t._2),
   *            i => (i.getStartMillis, i.getEndMillis))
   *     }}}
   *     In the above example we implicitly derive Coder[(Long, Long)] and we define two functions,
   *     one to convert a tuple (Long, Long) to Interval, and a second one to convert an Interval
   *     to a tuple of (Long, Long)
   */
  def xmap[A, B](c: Coder[A])(f: A => B, t: B => A): Coder[B] = {
    @inline def toB(bc: BCoder[A]) = new AtomicCoder[B] {
      override def encode(value: B, os: OutputStream): Unit =
        bc.encode(t(value), os)
      override def decode(is: InputStream): B =
        f(bc.decode(is))

      // delegate methods for determinism and equality checks
      override def verifyDeterministic(): Unit = bc.verifyDeterministic()
      override def consistentWithEquals(): Boolean = bc.consistentWithEquals()
      override def structuralValue(value: B): AnyRef =
        if (consistentWithEquals()) {
          value.asInstanceOf[AnyRef]
        } else {
          bc.structuralValue(t(value))
        }

      // delegate methods for byte size estimation
      override def isRegisterByteSizeObserverCheap(value: B): Boolean =
        bc.isRegisterByteSizeObserverCheap(t(value))
      override def registerByteSizeObserver(value: B, observer: ElementByteSizeObserver): Unit =
        bc.registerByteSizeObserver(t(value), observer)
    }
    Transform[A, B](c, bc => Coder.beam(toB(bc)))
  }

  private[scio] def record[T](
    typeName: String,
    cs: Array[(String, Coder[Any])],
    construct: Seq[Any] => T,
    destruct: T => Array[Any]
  ): Coder[T] =
    Record[T](typeName, cs, construct, destruct)
}

object Coder
    extends CoderGrammar
    with TupleCoders
    with AvroCoders
    with ProtobufCoders
    with AlgebirdCoders
    with JodaCoders
    with JavaBeanCoders
    with BeamTypeCoders
    with LowPriorityFallbackCoder {
  @inline final def apply[T](implicit c: Coder[T]): Coder[T] = c

  implicit def charCoder: Coder[Char] = ScalaCoders.charCoder
  implicit def byteCoder: Coder[Byte] = ScalaCoders.byteCoder
  implicit def stringCoder: Coder[String] = ScalaCoders.stringCoder
  implicit def shortCoder: Coder[Short] = ScalaCoders.shortCoder
  implicit def intCoder: Coder[Int] = ScalaCoders.intCoder
  implicit def longCoder: Coder[Long] = ScalaCoders.longCoder
  implicit def floatCoder: Coder[Float] = ScalaCoders.floatCoder
  implicit def doubleCoder: Coder[Double] = ScalaCoders.doubleCoder
  implicit def booleanCoder: Coder[Boolean] = ScalaCoders.booleanCoder
  implicit def unitCoder: Coder[Unit] = ScalaCoders.unitCoder
  implicit def nothingCoder: Coder[Nothing] = ScalaCoders.nothingCoder
  implicit def bigIntCoder: Coder[BigInt] = ScalaCoders.bigIntCoder
  implicit def bigDecimalCoder: Coder[BigDecimal] = ScalaCoders.bigDecimalCoder
  implicit def tryCoder[A: Coder]: Coder[Try[A]] = ScalaCoders.tryCoder
  implicit def eitherCoder[A: Coder, B: Coder]: Coder[Either[A, B]] = ScalaCoders.eitherCoder
  implicit def optionCoder[T, S[_] <: Option[_]](implicit c: Coder[T]): Coder[S[T]] =
    ScalaCoders.optionCoder
  implicit def noneCoder: Coder[None.type] = ScalaCoders.noneCoder
  implicit def bitSetCoder: Coder[BitSet] = ScalaCoders.bitSetCoder
  implicit def seqCoder[T: Coder]: Coder[Seq[T]] = ScalaCoders.seqCoder
  import shapeless.Strict
  implicit def pairCoder[A, B](implicit CA: Strict[Coder[A]], CB: Strict[Coder[B]]): Coder[(A, B)] =
    ScalaCoders.pairCoder
  implicit def iterableCoder[T: Coder]: Coder[Iterable[T]] = ScalaCoders.iterableCoder
  implicit def throwableCoder[T <: Throwable: ClassTag]: Coder[T] = ScalaCoders.throwableCoder
  implicit def listCoder[T: Coder]: Coder[List[T]] = ScalaCoders.listCoder
  implicit def traversableOnceCoder[T: Coder]: Coder[TraversableOnce[T]] =
    ScalaCoders.traversableOnceCoder
  implicit def setCoder[T: Coder]: Coder[Set[T]] = ScalaCoders.setCoder
  implicit def mutableSetCoder[T: Coder]: Coder[m.Set[T]] = ScalaCoders.mutableSetCoder
  implicit def vectorCoder[T: Coder]: Coder[Vector[T]] = ScalaCoders.vectorCoder
  implicit def arrayBufferCoder[T: Coder]: Coder[m.ArrayBuffer[T]] = ScalaCoders.arrayBufferCoder
  implicit def bufferCoder[T: Coder]: Coder[m.Buffer[T]] = ScalaCoders.bufferCoder
  implicit def listBufferCoder[T: Coder]: Coder[m.ListBuffer[T]] = ScalaCoders.listBufferCoder
  implicit def arrayCoder[T: Coder: ClassTag]: Coder[Array[T]] = ScalaCoders.arrayCoder
  implicit def arrayByteCoder: Coder[Array[Byte]] = ScalaCoders.arrayByteCoder
  implicit def wrappedArrayCoder[T: Coder: ClassTag](
    implicit wrap: Array[T] => m.WrappedArray[T]
  ): Coder[m.WrappedArray[T]] = ScalaCoders.wrappedArrayCoder
  implicit def mutableMapCoder[K: Coder, V: Coder]: Coder[m.Map[K, V]] = ScalaCoders.mutableMapCoder
  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] = ScalaCoders.mapCoder
  implicit def sortedSetCoder[T: Coder: Ordering]: Coder[SortedSet[T]] = ScalaCoders.sortedSetCoder

  implicit def voidCoder: Coder[Void] = JavaCoders.voidCoder
  implicit def uriCoder: Coder[java.net.URI] = JavaCoders.uriCoder
  implicit def pathCoder: Coder[java.nio.file.Path] = JavaCoders.pathCoder
  implicit def jIterableCoder[T](implicit c: Coder[T]): Coder[JIterable[T]] =
    JavaCoders.jIterableCoder
  implicit def jlistCoder[T](implicit c: Coder[T]): Coder[java.util.List[T]] = JavaCoders.jlistCoder
  implicit def jArrayListCoder[T](implicit c: Coder[T]): Coder[java.util.ArrayList[T]] =
    JavaCoders.jArrayListCoder
  implicit def jMapCoder[K, V](implicit ck: Coder[K], cv: Coder[V]): Coder[java.util.Map[K, V]] =
    JavaCoders.jMapCoder
  implicit def jTryCoder[A](implicit c: Coder[Try[A]]): Coder[BaseAsyncLookupDoFn.Try[A]] =
    JavaCoders.jTryCoder
  implicit def jBitSetCoder: Coder[java.util.BitSet] = JavaCoders.jBitSetCoder
  implicit val jShortCoder: Coder[java.lang.Short] = JavaCoders.jShortCoder
  implicit val jByteCoder: Coder[java.lang.Byte] = JavaCoders.jByteCoder
  implicit val jIntegerCoder: Coder[java.lang.Integer] = JavaCoders.jIntegerCoder
  implicit val jLongCoder: Coder[java.lang.Long] = JavaCoders.jLongCoder
  implicit val jFloatCoder: Coder[java.lang.Float] = JavaCoders.jFloatCoder
  implicit val jDoubleCoder: Coder[java.lang.Double] = JavaCoders.jDoubleCoder
  implicit val jBooleanCoder: Coder[java.lang.Boolean] = JavaCoders.jBooleanCoder
  implicit def jBigIntegerCoder: Coder[JBigInteger] = JavaCoders.jBigIntegerCoder
  implicit def jBigDecimalCoder: Coder[JBigDecimal] = JavaCoders.jBigDecimalCoder
  implicit def serializableCoder: Coder[Serializable] = Coder.kryo[Serializable]
  implicit def jInstantCoder: Coder[Instant] = JavaCoders.jInstantCoder
  implicit def coderJEnum[E <: java.lang.Enum[E]: ClassTag]: Coder[E] = JavaCoders.coderJEnum
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
    additionalMessage: Option[String],
    baseStack: Array[StackTraceElement]
  ): T = {
    cause.printStackTrace()
    val messageItem = additionalMessage.map { msg =>
      new StackTraceElement(s"Due to $msg", "", "", 0)
    }

    val adjustedStack = messageItem ++ cause.getStackTrace ++ baseStack
    cause.setStackTrace(adjustedStack.toArray)
    cause
  }
}
