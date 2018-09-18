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

import com.google.common.collect.Lists
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder, _}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

import scala.reflect.ClassTag
import scala.collection.{SortedSet, TraversableOnce, mutable => m}
import scala.collection.convert.Wrappers
import scala.language.higherKinds

private object UnitCoder extends AtomicCoder[Unit] {
  def encode(value: Unit, os: OutputStream): Unit = ()
  def decode(is: InputStream): Unit = ()
}

private object NothingCoder extends AtomicCoder[Nothing] {
  def encode(value: Nothing, os: OutputStream): Unit = ()
  def decode(is: InputStream): Nothing = ??? // can't possibly happen
}

private abstract class BaseSeqLikeCoder[M[_], T](val elemCoder: BCoder[T])
                                                (implicit toSeq: M[T] => TraversableOnce[T])
  extends AtomicCoder[M[T]] {
  override def getCoderArguments: java.util.List[_ <: BCoder[_]] = Lists.newArrayList(elemCoder)

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit = elemCoder.verifyDeterministic()
  override def consistentWithEquals(): Boolean = elemCoder.consistentWithEquals()
  override def structuralValue(value: M[T]): AnyRef = {
    val b = Seq.newBuilder[AnyRef]
    value.foreach(v => b += elemCoder.structuralValue(v))
    b.result()
  }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: M[T]): Boolean = false
  override def registerByteSizeObserver(value: M[T], observer: ElementByteSizeObserver): Unit = {
    if (value.isInstanceOf[Wrappers.JIterableWrapper[_]]) {
      val wrapper = value.asInstanceOf[Wrappers.JIterableWrapper[T]]
      IterableCoder.of(elemCoder).registerByteSizeObserver(wrapper.underlying, observer)
    } else {
      super.registerByteSizeObserver(value, observer)
    }
  }
}

private abstract class SeqLikeCoder[M[_], T](bc: BCoder[T])
                                            (implicit toSeq: M[T] => TraversableOnce[T])
  extends BaseSeqLikeCoder[M, T](bc) {
  private val lc = VarIntCoder.of()
  override def encode(value: M[T], outStream: OutputStream): Unit = {
    lc.encode(value.size, outStream)
    value.foreach(bc.encode(_, outStream))
  }
  def decode(inStream: InputStream, builder: scala.collection.mutable.Builder[T, M[T]]): M[T] = {
    val size = lc.decode(inStream)
    (1 to size).foreach(_ => builder += bc.decode(inStream))
    builder.result()
  }
}

private class OptionCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Option, T](bc) {
  private val bcoder = BooleanCoder.of().asInstanceOf[BCoder[Boolean]]
  override def encode(value: Option[T], os: OutputStream): Unit = {
    bcoder.encode(value.isDefined, os)
    value.foreach { bc.encode(_, os) }
  }

  override def decode(is: InputStream): Option[T] = {
    val isDefined = bcoder.decode(is)
    if (isDefined) Some(bc.decode(is)) else None
  }
}

private class SeqCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Seq, T](bc) {
  override def decode(inStream: InputStream): Seq[T] = decode(inStream, Seq.newBuilder[T])
}

private class ListCoder[T](bc: BCoder[T]) extends SeqLikeCoder[List, T](bc) {
  override def decode(inStream: InputStream): List[T] = decode(inStream, List.newBuilder[T])
}

// TODO: implement chunking
private class TraversableOnceCoder[T](bc: BCoder[T]) extends SeqLikeCoder[TraversableOnce, T](bc) {
  override def decode(inStream: InputStream): TraversableOnce[T] =
    decode(inStream, Seq.newBuilder[T])
}

// TODO: implement chunking
private class IterableCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Iterable, T](bc) {
  def decode(inStream: InputStream): Iterable[T] = decode(inStream, Iterable.newBuilder[T])
}

private class VectorCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Vector, T](bc) {
  def decode(inStream: InputStream): Vector[T] = decode(inStream, Vector.newBuilder[T])
}

private class ArrayCoder[T: ClassTag](bc: BCoder[T]) extends SeqLikeCoder[Array, T](bc) {
  def decode(inStream: InputStream): Array[T] = decode(inStream, Array.newBuilder[T])
}

private class ArrayBufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.ArrayBuffer, T](bc) {
  def decode(inStream: InputStream): m.ArrayBuffer[T] =
    decode(inStream, m.ArrayBuffer.newBuilder[T])
}

private class BufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.Buffer, T](bc) {
  def decode(inStream: InputStream): m.Buffer[T] = decode(inStream, m.Buffer.newBuilder[T])
}

private class SetCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Set, T](bc) {
  def decode(inStream: InputStream): Set[T] = decode(inStream, Set.newBuilder[T])
}

private class SortedSetCoder[T: Ordering](bc: BCoder[T]) extends SeqLikeCoder[SortedSet, T](bc) {
  def decode(inStream: InputStream): SortedSet[T] = decode(inStream, SortedSet.newBuilder[T])
}

private class MapCoder[K, V](kc: BCoder[K], vc: BCoder[V]) extends AtomicCoder[Map[K, V]] {
  private val lc = VarIntCoder.of()

  def encode(value: Map[K, V], os: OutputStream): Unit = {
    lc.encode(value.size, os)
    value.foreach { case (k, v) =>
      kc.encode(k, os)
      vc.encode(v, os)
    }
  }

  def decode(is: InputStream): Map[K, V] = {
    val l = lc.decode(is)
    val builder = Map.newBuilder[K, V]
    (1 to l).map { _ =>
      val k = kc.decode(is)
      val v = vc.decode(is)
      builder += (k -> v)
    }
    builder.result()
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this, "Ordering of entries in a Map may be non-deterministic.")
  override def consistentWithEquals(): Boolean = false

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: Map[K, V]): Boolean = false
  override def registerByteSizeObserver(value: Map[K, V],
                                        observer: ElementByteSizeObserver): Unit = {
    lc.registerByteSizeObserver(value.size, observer)
    value.foreach { case (k, v) =>
      kc.registerByteSizeObserver(k, observer)
      vc.registerByteSizeObserver(v, observer)
    }
  }
}

private class MutableMapCoder[K, V](kc: BCoder[K], vc: BCoder[V]) extends AtomicCoder[m.Map[K, V]] {
  private val lc = VarIntCoder.of()

  override def encode(value: m.Map[K, V], os: OutputStream): Unit = {
    lc.encode(value.size, os)
    value.foreach { case (k, v) =>
      kc.encode(k, os)
      vc.encode(v, os)
    }
  }

  override def decode(is: InputStream): m.Map[K, V] = {
    val l = lc.decode(is)
    val builder = m.Map.newBuilder[K, V]
    (1 to l).map { _ =>
      val k = kc.decode(is)
      val v = vc.decode(is)
      builder += (k -> v)
    }
    builder.result()
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this, "Ordering of entries in a Map may be non-deterministic.")
  override def consistentWithEquals(): Boolean = false

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: m.Map[K, V]): Boolean = false
  override def registerByteSizeObserver(value: m.Map[K, V],
                                        observer: ElementByteSizeObserver): Unit = {
    lc.registerByteSizeObserver(value.size, observer)
    value.foreach { case (k, v) =>
      kc.registerByteSizeObserver(k, observer)
      vc.registerByteSizeObserver(v, observer)
    }
  }
}

trait ScalaCoders {
  // TODO: support all primitive types
  // BigDecimalCoder
  // BigIntegerCoder
  // BitSetCoder
  // BooleanCoder
  // ByteStringCoder

  // DurationCoder
  // InstantCoder

  // TableRowJsonCoder

  // implicit def traversableCoder[T](implicit c: Coder[T]): Coder[TraversableOnce[T]] = ???
  implicit def seqCoder[T: Coder]: Coder[Seq[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new SeqCoder[T](bc))
    }

  // TODO: proper chunking implementation
  implicit def iterableCoder[T: Coder]: Coder[Iterable[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new IterableCoder[T](bc))
    }

  implicit def throwableCoder[T <: Throwable: ClassTag]: Coder[T] =
    Coder.kryo[T]

  // specialized coder. Since `::` is a case class, Magnolia would derive an incorrect one...
  implicit def listCoder[T: Coder]: Coder[List[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new ListCoder[T](bc))
    }

  implicit def traversableOnceCoder[T: Coder]: Coder[TraversableOnce[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new TraversableOnceCoder[T](bc))
    }

  implicit def setCoder[T: Coder]: Coder[Set[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new SetCoder[T](bc))
    }

  implicit def vectorCoder[T: Coder]: Coder[Vector[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new VectorCoder[T](bc))
    }

  implicit def arrayBufferCoder[T: Coder]: Coder[m.ArrayBuffer[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new ArrayBufferCoder[T](bc))
    }

  implicit def bufferCoder[T: Coder]: Coder[m.Buffer[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new BufferCoder[T](bc))
    }

  implicit def arrayCoder[T: Coder: ClassTag]: Coder[Array[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new ArrayCoder[T](bc))
    }

  implicit def arrayByteCoder: Coder[Array[Byte]] =
    Coder.beam(ByteArrayCoder.of())

  implicit def mutableMapCoder[K: Coder, V: Coder]: Coder[m.Map[K, V]] =
    Coder.transform(Coder[K]) { kc =>
      Coder.transform(Coder[V]) { vc =>
        Coder.beam(new MutableMapCoder[K, V](kc, vc))
      }
    }

  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] =
    Coder.transform(Coder[K]) { kc =>
      Coder.transform(Coder[V]) { vc =>
        Coder.beam(new MapCoder[K, V](kc, vc))
      }
    }

  implicit def sortedSetCoder[T: Coder: Ordering]: Coder[SortedSet[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new SortedSetCoder[T](bc))
    }

  // implicit def enumerationCoder[E <: Enumeration]: Coder[E#Value] = ???
}
