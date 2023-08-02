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
import org.apache.beam.sdk.coders.{Coder => BCoder, CustomCoder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

import java.util.{List => JList}

sealed abstract private[scio] class WrappedCoder[T] extends CustomCoder[T] {
  def bcoder: BCoder[T]

  override def equals(obj: Any): Boolean = obj match {
    case that: WrappedCoder[_] => bcoder == that.bcoder
    case _                     => false
  }

  override def hashCode(): Int = bcoder.hashCode

  override def getCoderArguments: JList[_ <: BCoder[_]] =
    bcoder.getCoderArguments

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

  @transient override lazy val bcoder: BCoder[T] = bc

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
