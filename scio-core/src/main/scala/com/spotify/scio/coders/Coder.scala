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

package com.spotify.scio.coders

import java.io.{InputStream, OutputStream}

import com.spotify.scio.coders.instances.Implicits
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{AtomicCoder, Coder => BCoder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.apache.beam.sdk.values.KV

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

// scalastyle:off line.size.limit
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
""")
sealed trait Coder[T] extends Serializable
// scalastyle:on line.size.limit
final case class Beam[T] private (beam: BCoder[T]) extends Coder[T] {
  override def toString: String = s"Beam($beam)"
}
final case class Fallback[T] private (ct: ClassTag[T]) extends Coder[T] {
  override def toString: String = s"Fallback($ct)"
}
final case class Transform[A, B] private (c: Coder[A], f: BCoder[A] => Coder[B]) extends Coder[B] {
  override def toString: String = s"Transform($c, $f)"
}
final case class Disjunction[T, Id] private (typeName: String,
                                             idCoder: Coder[Id],
                                             id: T => Id,
                                             coder: Map[Id, Coder[T]])
    extends Coder[T] {
  override def toString: String = s"Disjunction($typeName, $coder)"
}

final case class Record[T] private (typeName: String,
                                    cs: Array[(String, Coder[Any])],
                                    construct: Seq[Any] => T,
                                    destruct: T => Array[Any])
    extends Coder[T] {
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

private final case class DisjunctionCoder[T, Id](typeName: String,
                                                 idCoder: BCoder[Id],
                                                 id: T => Id,
                                                 coders: Map[Id, BCoder[T]])
    extends AtomicCoder[T] {
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
}

final case class CoderException private[coders] (stacktrace: Array[StackTraceElement],
                                                 cause: Throwable,
                                                 message: String = "")
    extends RuntimeException {
  override def getMessage: String = cause.getMessage
  override def getCause: Throwable = new RuntimeException {
    override def getMessage: String = s"$message - Coder was materialized at"
    override def getStackTrace: Array[StackTraceElement] = stacktrace
    override def getCause: Throwable = cause.getCause
  }
  override def getStackTrace: Array[StackTraceElement] = cause.getStackTrace
}

private[coders] object CoderException {
  def prepareStackTrace: Array[StackTraceElement] =
    Thread
      .currentThread()
      .getStackTrace()
      .dropWhile(!_.getClassName.contains(CoderMaterializer.getClass.getName))
      .take(10)
}

// XXX: Workaround a NPE deep down the stack in Beam
// info]   java.lang.NullPointerException: null value in entry: T=null
private case class WrappedBCoder[T](u: BCoder[T]) extends BCoder[T] {

  /**
   * Eagerly compute a stack trace on materialization
   * to provide a helpful stacktrace if an exception happens
   */
  private val stackTrace: Array[StackTraceElement] =
    CoderException.prepareStackTrace

  private def buildException(cause: Throwable): Exception =
    CoderException(stackTrace, cause)

  override def toString: String = u.toString

  @inline private def catching[A](a: => A) =
    try { a } catch {
      case ex: Throwable =>
        throw buildException(ex)
    }

  override def encode(value: T, os: OutputStream): Unit =
    catching { u.encode(value, os) }

  override def decode(is: InputStream): T =
    catching { u.decode(is) }

  override def decode(inStream: InputStream, context: BCoder.Context): T =
    catching { u.decode(inStream, context) }

  override def encode(value: T, outStream: OutputStream, context: BCoder.Context): Unit =
    catching { u.encode(value, outStream, context) }

  override def getCoderArguments: java.util.List[_ <: BCoder[_]] = u.getCoderArguments

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit = u.verifyDeterministic()
  override def consistentWithEquals(): Boolean = u.consistentWithEquals()
  override def structuralValue(value: T): AnyRef = u.structuralValue(value)

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: T): Boolean =
    u.isRegisterByteSizeObserverCheap(value)
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit =
    u.registerByteSizeObserver(value, observer)
}

private object WrappedBCoder {
  def create[T](u: BCoder[T]): BCoder[T] =
    u match {
      case WrappedBCoder(_) => u
      case _                => new WrappedBCoder(u)
    }
}

// Coder used internally specifically for Magnolia derived coders.
// It's technically possible to define Product coders only in terms of `Coder.transform`
// This is just faster
private class RecordCoder[T](typeName: String,
                             cs: Array[(String, BCoder[Any])],
                             construct: Seq[Any] => T,
                             destruct: T => Array[Any])
    extends AtomicCoder[T] {
  @inline def onErrorMsg[A](msg: => String)(f: => A): A =
    try { f } catch {
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
        // scalastyle:off line.size.limit
        s"Exception while trying to `encode` an instance of $typeName:  Can't encode field $label value $v") {
        // scalastyle:on line.size.limit
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
        s"Exception while trying to `decode` an instance of $typeName: Can't decode field $label") {
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
  override def structuralValue(value: T): AnyRef = {
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

sealed trait CoderGrammar {
  def beam[T](beam: BCoder[T]): Coder[T] =
    Beam(beam)
  def kv[K, V](koder: Coder[K], voder: Coder[V]): Coder[KV[K, V]] =
    KVCoder(koder, voder)
  def kryo[T](implicit ct: ClassTag[T]): Coder[T] =
    Fallback[T](ct)
  def transform[A, B](c: Coder[A])(f: BCoder[A] => Coder[B]): Coder[B] =
    Transform(c, f)
  def disjunction[T, Id: Coder](typeName: String, coder: Map[Id, Coder[T]])(id: T => Id): Coder[T] =
    Disjunction(typeName, Coder[Id], id, coder)
  def xmap[A, B](c: Coder[A])(f: A => B, t: B => A): Coder[B] = {
    @inline def toB(bc: BCoder[A]) = new AtomicCoder[B] {
      override def encode(value: B, os: OutputStream): Unit =
        bc.encode(t(value), os)
      override def decode(is: InputStream): B =
        f(bc.decode(is))

      // delegate methods for determinism and equality checks
      override def verifyDeterministic(): Unit = bc.verifyDeterministic()
      override def consistentWithEquals(): Boolean = bc.consistentWithEquals()
      override def structuralValue(value: B): AnyRef = bc.structuralValue(t(value))

      // delegate methods for byte size estimation
      override def isRegisterByteSizeObserverCheap(value: B): Boolean =
        bc.isRegisterByteSizeObserverCheap(t(value))
      override def registerByteSizeObserver(value: B, observer: ElementByteSizeObserver): Unit =
        bc.registerByteSizeObserver(t(value), observer)
    }
    Transform[A, B](c, bc => Coder.beam(toB(bc)))
  }

  private[scio] def record[T](typeName: String,
                              cs: Array[(String, Coder[Any])],
                              construct: Seq[Any] => T,
                              destruct: T => Array[Any]): Coder[T] =
    Record[T](typeName, cs, construct, destruct)
}

object Coder extends CoderGrammar with Implicits {
  @inline final def apply[T](implicit c: Coder[T]): Coder[T] = c
}
