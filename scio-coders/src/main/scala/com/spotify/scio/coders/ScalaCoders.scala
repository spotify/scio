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
import org.apache.beam.sdk.coders.{ Coder => BCoder, _}
import scala.reflect.ClassTag
import scala.collection.{ mutable => m }
import scala.collection.SortedSet

private final object UnitCoder extends AtomicCoder[Unit]{
  def encode(value: Unit, os: OutputStream): Unit = ()
  def decode(is: InputStream): Unit = ()
}

private final object NothingCoder extends AtomicCoder[Nothing] {
  def encode(value: Nothing, os: OutputStream): Unit = ()
  def decode(is: InputStream): Nothing = ??? // can't possibly happen
}

private class OptionCoder[T](tc: BCoder[T]) extends AtomicCoder[Option[T]] {
  val bcoder = BooleanCoder.of().asInstanceOf[BCoder[Boolean]]
  def encode(value: Option[T], os: OutputStream): Unit = {
    bcoder.encode(value.isDefined, os)
    value.foreach { tc.encode(_, os) }
  }

  def decode(is: InputStream): Option[T] =
    Option(bcoder.decode(is)).collect {
      case true => tc.decode(is)
    }
}

private class SeqCoder[T](bc: BCoder[T]) extends AtomicCoder[Seq[T]] {
  val lc = VarIntCoder.of()
  def decode(in: InputStream): Seq[T] = {
    val l = lc.decode(in)
    (1 to l).map { _ =>
      bc.decode(in)
    }
  }

  def encode(ts: Seq[T], out: OutputStream): Unit = {
    lc.encode(ts.length, out)
    ts.foreach { v => bc.encode(v, out) }
  }
}

private class ListCoder[T](bc: BCoder[T]) extends AtomicCoder[List[T]] {
  val seqCoder = new SeqCoder[T](bc)
  def encode(value: List[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): List[T] =
    seqCoder.decode(is).toList
}

private class TraversableOnceCoder[T](bc: BCoder[T]) extends AtomicCoder[TraversableOnce[T]] {
  val seqCoder = new SeqCoder[T](bc)
  def encode(value: TraversableOnce[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): TraversableOnce[T] =
    seqCoder.decode(is)
}

private class IterableCoder[T](bc: BCoder[T]) extends AtomicCoder[Iterable[T]] {
  val seqCoder = new SeqCoder[T](bc)
  def encode(value: Iterable[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): Iterable[T] =
    seqCoder.decode(is)
}

private class VectorCoder[T](bc: BCoder[T]) extends AtomicCoder[Vector[T]] {
  val seqCoder = new SeqCoder[T](bc)
  def encode(value: Vector[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): Vector[T] =
    seqCoder.decode(is).toVector
}

private class ArrayCoder[T : ClassTag](bc: BCoder[T]) extends AtomicCoder[Array[T]] {
  val seqCoder = new SeqCoder[T](bc)
  def encode(value: Array[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): Array[T] =
    seqCoder.decode(is).toArray
}

private class ArrayBufferCoder[T](c: BCoder[T]) extends AtomicCoder[m.ArrayBuffer[T]] {
  val seqCoder = new SeqCoder[T](c)
  def encode(value: m.ArrayBuffer[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): m.ArrayBuffer[T] =
    m.ArrayBuffer(seqCoder.decode(is):_*)
}

private class SetCoder[T](c: BCoder[T]) extends AtomicCoder[Set[T]] {
  val seqCoder = new SeqCoder[T](c)
  def encode(value: Set[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): Set[T] =
    Set(seqCoder.decode(is):_*)
}

private class SortedSetCoder[T: Ordering](c: BCoder[T]) extends AtomicCoder[SortedSet[T]] {
  val seqCoder = new SeqCoder[T](c)
  def encode(value: SortedSet[T], os: OutputStream): Unit =
    seqCoder.encode(value.toSeq, os)
  def decode(is: InputStream): SortedSet[T] =
    SortedSet(seqCoder.decode(is):_*)
}

private class MapCoder[K, V](kc: BCoder[K], vc: BCoder[V]) extends AtomicCoder[Map[K, V]] {
  val lc = VarIntCoder.of()
  def decode(in: InputStream): Map[K, V] = {
    val l = lc.decode(in)
    (1 to l).map { _ =>
      val k = kc.decode(in)
      val v = vc.decode(in)
      (k, v)
    }.toMap
  }

  def encode(ts: Map[K, V], out: OutputStream): Unit = {
    lc.encode(ts.size, out)
    ts.foreach { case (k, v) =>
      kc.encode(k, out)
      vc.encode(v, out)
    }
  }
}

private class MutableMapCoder[K, V](kc: BCoder[K], vc: BCoder[V]) extends AtomicCoder[m.Map[K, V]] {
  val lc = VarIntCoder.of()
  def decode(in: InputStream): m.Map[K, V] = {
    val l = lc.decode(in)
    m.Map((1 to l).map { _ =>
      val k = kc.decode(in)
      val v = vc.decode(in)
      (k, v)
    }:_*)
  }

  def encode(ts: m.Map[K, V], out: OutputStream): Unit = {
    lc.encode(ts.size, out)
    ts.foreach { case (k, v) =>
      kc.encode(k, out)
      vc.encode(v, out)
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
     Coder.transform(Coder[T]){ bc => Coder.beam(new SeqCoder[T](bc)) }

  // TODO: proper chunking implementation
  implicit def iterableCoder[T: Coder]: Coder[Iterable[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new IterableCoder[T](bc)) }

  implicit def throwableCoder[T <: Throwable : ClassTag]: Coder[T] = Coder.kryo[T]

  // specialized coder. Since `::` is a case class, Magnolia would derive an incorrect one...
  implicit def listCoder[T: Coder]: Coder[List[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new ListCoder[T](bc)) }

  implicit def traversableOnceCoder[T: Coder]: Coder[TraversableOnce[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new TraversableOnceCoder[T](bc)) }

  implicit def setCoder[T: Coder]: Coder[Set[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new SetCoder[T](bc)) }

  implicit def vectorCoder[T: Coder]: Coder[Vector[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new VectorCoder[T](bc)) }

  implicit def arraybufferCoder[T: Coder]: Coder[m.ArrayBuffer[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new ArrayBufferCoder[T](bc)) }

  implicit def bufferCoder[T: Coder]: Coder[scala.collection.mutable.Buffer[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.xmap(Coder.beam(new SeqCoder[T](bc)))(_.toBuffer, _.toSeq) // Buffer <: Seq
    }

  implicit def arrayCoder[T: Coder : ClassTag]: Coder[Array[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new ArrayCoder[T](bc)) }

  implicit def arrayByteCoder: Coder[Array[Byte]] = Coder.beam(ByteArrayCoder.of())

  implicit def mutableMapCoder[K: Coder, V: Coder]: Coder[m.Map[K, V]] =
    Coder.transform(Coder[K]){ kc =>
      Coder.transform(Coder[V]){ vc =>
        Coder.beam(new MutableMapCoder[K, V](kc, vc))
      }
    }

  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] =
    Coder.transform(Coder[K]){ kc =>
      Coder.transform(Coder[V]){ vc =>
        Coder.beam(new MapCoder[K, V](kc, vc))
      }
    }

  implicit def sortedSetCoder[T: Coder : Ordering]: Coder[SortedSet[T]] =
    Coder.transform(Coder[T]){ bc => Coder.beam(new SortedSetCoder[T](bc)) }

  // implicit def enumerationCoder[E <: Enumeration]: Coder[E#Value] = ???
}
