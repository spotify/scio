/*
 * Copyright 2023 Spotify AB.
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
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder, CustomCoder, StructuredCoder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

import java.util.{List => JList, Objects}
import scala.jdk.CollectionConverters._

///////////////////////////////////////////////////////////////////////////////
// Materialized beam coders
///////////////////////////////////////////////////////////////////////////////
final private[coders] class SingletonCoder[T](
  val typeName: String,
  supply: () => T
) extends CustomCoder[T] {
  @transient private lazy val singleton = supply()

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
) extends StructuredCoder[T] {

  override def getCoderArguments: JList[_ <: BCoder[_]] = cs.map(_._2).asJava

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

  override def toString: String = s"TransformCoder[$typeName]($bcoder)"

  override def equals(obj: Any): Boolean = obj match {
    case that: TransformCoder[_, _] =>
      typeName == that.typeName && bcoder == that.bcoder
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(typeName, bcoder)
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
