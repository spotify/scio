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

package com.spotify.scio.coders.instances

import java.io.{InputStream, OutputStream}
import java.util
import java.util.Collections

import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder, IterableCoder => BIterableCoder, _}
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.apache.beam.sdk.util.BufferedElementCountingOutputStream
import org.apache.beam.sdk.util.VarInt

import scala.reflect.ClassTag
import scala.collection.{BitSet, SortedSet, mutable => m}
import scala.util.Try
import scala.collection.compat._
import scala.collection.compat.extra.Wrappers
import scala.collection.AbstractIterable

private object UnitCoder extends AtomicCoder[Unit] {
  override def encode(value: Unit, os: OutputStream): Unit = ()
  override def decode(is: InputStream): Unit = ()
}

private object NothingCoder extends AtomicCoder[Nothing] {
  override def encode(value: Nothing, os: OutputStream): Unit = ()
  override def decode(is: InputStream): Nothing =
    // can't possibly happen
    throw new IllegalStateException("Trying to decode a value of type Nothing is impossible")
  override def consistentWithEquals(): Boolean = true
}

abstract private[coders] class BaseSeqLikeCoder[M[_], T](val elemCoder: BCoder[T])
    extends AtomicCoder[M[T]] {
  override def getCoderArguments: java.util.List[_ <: BCoder[_]] =
    Collections.singletonList(elemCoder)

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit = elemCoder.verifyDeterministic()
  override def consistentWithEquals(): Boolean = elemCoder.consistentWithEquals()

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: M[T]): Boolean = false
  override def registerByteSizeObserver(value: M[T], observer: ElementByteSizeObserver): Unit =
    if (value.isInstanceOf[Wrappers.JIterableWrapper[_]]) {
      val wrapper = value.asInstanceOf[Wrappers.JIterableWrapper[T]]
      BIterableCoder.of(elemCoder).registerByteSizeObserver(wrapper.underlying, observer)
    } else {
      super.registerByteSizeObserver(value, observer)
    }
}

abstract private class SeqLikeCoder[M[_], T](bc: BCoder[T])(implicit
  ev: M[T] => IterableOnce[T]
) extends BaseSeqLikeCoder[M, T](bc) {
  override def encode(value: M[T], outStream: OutputStream): Unit = {
    val traversable = ev(value)
    VarInt.encode(traversable.iterator.size, outStream)
    traversable.iterator.foreach(bc.encode(_, outStream))
  }

  def decode(inStream: InputStream, builder: m.Builder[T, M[T]]): M[T] = {
    val size = VarInt.decodeInt(inStream)
    builder.sizeHint(size)
    var i = 0
    while (i < size) {
      builder += bc.decode(inStream)
      i += 1
    }
    builder.result()
  }

  override def structuralValue(value: M[T]): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      val b = Seq.newBuilder[AnyRef]
      val traversable = ev(value)
      b.sizeHint(traversable.iterator.size)
      traversable.iterator.foreach(v => b += elemCoder.structuralValue(v))
      b.result()
    }

  override def toString: String = s"SeqLikeCoder($bc)"
}

abstract private class BufferedSeqLikeCoder[M[_], T](bc: BCoder[T])(implicit
  ev: M[T] => IterableOnce[T]
) extends BaseSeqLikeCoder[M, T](bc) {

  override def encode(value: M[T], outStream: OutputStream): Unit = {
    val buff = new BufferedElementCountingOutputStream(outStream)
    ev(value).iterator.foreach { elem =>
      buff.markElementStart()
      bc.encode(elem, buff)
    }
    buff.finish()
  }

  def decode(inStream: InputStream, builder: m.Builder[T, M[T]]): M[T] = {
    var count = VarInt.decodeLong(inStream);
    while (count > 0L) {
      builder += bc.decode(inStream)
      count -= 1
      if (count == 0L) {
        count = VarInt.decodeLong(inStream);
      }
    }
    builder.result()
  }

  override def structuralValue(value: M[T]): AnyRef = {
    val b = Seq.newBuilder[AnyRef]
    ev(value).iterator.foreach(v => b += elemCoder.structuralValue(v))
    b.result()
  }

  override def toString: String = s"BufferedSeqLikeCoder($bc)"
}

private class OptionCoder[T](bc: BCoder[T]) extends AtomicCoder[Option[T]] {
  private[this] val bcoder = BooleanCoder.of()

  override def encode(value: Option[T], os: OutputStream): Unit = {
    bcoder.encode(value.isDefined, os)
    value.foreach(bc.encode(_, os))
  }

  override def decode(is: InputStream): Option[T] = {
    val isDefined = bcoder.decode(is)
    if (isDefined) Some(bc.decode(is)) else None
  }

  override def getCoderArguments: java.util.List[_ <: BCoder[_]] =
    Collections.singletonList(bc)

  override def verifyDeterministic(): Unit = bc.verifyDeterministic()

  override def consistentWithEquals(): Boolean = bc.consistentWithEquals()

  override def toString: String = s"OptionCoder($bc)"
}

private class SeqCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Seq, T](bc) {
  override def decode(inStream: InputStream): Seq[T] = decode(inStream, Seq.newBuilder[T])
}

private class ListCoder[T](bc: BCoder[T]) extends SeqLikeCoder[List, T](bc) {
  override def decode(inStream: InputStream): List[T] = decode(inStream, List.newBuilder[T])
}

private class IterableOnceCoder[T](bc: BCoder[T])
    extends BufferedSeqLikeCoder[IterableOnce, T](bc) {
  override def decode(inStream: InputStream): IterableOnce[T] =
    decode(inStream, Seq.newBuilder[T])
}

private class IterableCoder[T](bc: BCoder[T]) extends BufferedSeqLikeCoder[Iterable, T](bc) {
  override def decode(inStream: InputStream): Iterable[T] =
    IterableCoder.IterableWrapper(decode(inStream, Iterable.newBuilder[T]))
}

private object IterableCoder {
  final case class IterableWrapper[T](underlying: Iterable[T]) extends AbstractIterable[T] {
    override def iterator = underlying.iterator
  }
}

private class VectorCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Vector, T](bc) {
  override def decode(inStream: InputStream): Vector[T] = decode(inStream, Vector.newBuilder[T])
}

// TODO: restore once https://github.com/lampepfl/dotty/issues/10599 is fixed
// private class ArrayCoder[@specialized(Short, Int, Long, Float, Double, Boolean, Char) T: ClassTag](
//   bc: BCoder[T]
// ) extends SeqLikeCoder[Array, T](bc) {
//   override def decode(inStream: InputStream): Array[T] = {
//     val size = VarInt.decodeInt(inStream)
//     val arr = new Array[T](size)
//     var i = 0
//     while (i < size) {
//       arr(i) = bc.decode(inStream)
//       i += 1
//     }
//     arr
//   }
//   override def consistentWithEquals(): Boolean = false
// }

private class ArrayBufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.ArrayBuffer, T](bc) {
  override def decode(inStream: InputStream): m.ArrayBuffer[T] =
    decode(inStream, m.ArrayBuffer.newBuilder[T])
}

private class BufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.Buffer, T](bc) {
  override def decode(inStream: InputStream): m.Buffer[T] = decode(inStream, m.Buffer.newBuilder[T])
}

private class SetCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Set, T](bc) {
  override def decode(inStream: InputStream): Set[T] = decode(inStream, Set.newBuilder[T])

  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of entries in a Set may be non-deterministic."
    )
}

private class MutableSetCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.Set, T](bc) {
  override def decode(inStream: InputStream): m.Set[T] = decode(inStream, m.Set.newBuilder[T])
}

private class SortedSetCoder[T: Ordering](bc: BCoder[T]) extends SeqLikeCoder[SortedSet, T](bc) {
  override def decode(inStream: InputStream): SortedSet[T] =
    decode(inStream, SortedSet.newBuilder[T])
}

private class BitSetCoderInternal extends AtomicCoder[BitSet] {
  private[this] val lc = VarIntCoder.of()

  def decode(in: InputStream): BitSet = {
    val l = lc.decode(in)
    val builder = BitSet.newBuilder
    builder.sizeHint(l)
    (1 to l).foreach(_ => builder += lc.decode(in))

    builder.result()
  }

  def encode(ts: BitSet, out: OutputStream): Unit = {
    lc.encode(ts.size, out)
    ts.foreach(v => lc.encode(v, out))
  }
}

private[coders] class MapCoder[K, V](val kc: BCoder[K], val vc: BCoder[V])
    extends AtomicCoder[Map[K, V]] {
  private[this] val lc = VarIntCoder.of()

  override def encode(value: Map[K, V], os: OutputStream): Unit = {
    lc.encode(value.size, os)
    val it = value.iterator
    while (it.hasNext) {
      val (k, v) = it.next()
      kc.encode(k, os)
      vc.encode(v, os)
    }
  }

  override def decode(is: InputStream): Map[K, V] = {
    val l = lc.decode(is)
    val builder = Map.newBuilder[K, V]
    builder.sizeHint(l)
    var i = 0
    while (i < l) {
      val k = kc.decode(is)
      val v = vc.decode(is)
      builder += (k -> v)
      i = i + 1
    }
    builder.result()
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of entries in a Map may be non-deterministic."
    )
  override def consistentWithEquals(): Boolean =
    kc.consistentWithEquals() && vc.consistentWithEquals()
  override def structuralValue(value: Map[K, V]): AnyRef =
    if (consistentWithEquals()) {
      value
    } else {
      val b = Map.newBuilder[Any, Any]
      b.sizeHint(value.size)
      value.foreach { case (k, v) =>
        b += kc.structuralValue(k) -> vc.structuralValue(v)
      }
      b.result()
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: Map[K, V]): Boolean = false
  override def registerByteSizeObserver(
    value: Map[K, V],
    observer: ElementByteSizeObserver
  ): Unit = {
    lc.registerByteSizeObserver(value.size, observer)
    value.foreach { case (k, v) =>
      kc.registerByteSizeObserver(k, observer)
      vc.registerByteSizeObserver(v, observer)
    }
  }

  override def toString: String =
    s"MapCoder($kc, $vc)"
}

private class MutableMapCoder[K, V](kc: BCoder[K], vc: BCoder[V]) extends AtomicCoder[m.Map[K, V]] {
  private[this] val lc = VarIntCoder.of()

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
    builder.sizeHint(l)
    var i = 0
    while (i < l) {
      val k = kc.decode(is)
      val v = vc.decode(is)
      builder += (k -> v)
      i = i + 1
    }
    builder.result()
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of entries in a Map may be non-deterministic."
    )
  override def consistentWithEquals(): Boolean =
    kc.consistentWithEquals() && vc.consistentWithEquals()
  override def structuralValue(value: m.Map[K, V]): AnyRef =
    if (consistentWithEquals()) {
      value
    } else {
      val b = m.Map.newBuilder[Any, Any]
      b.sizeHint(value.size)
      value.foreach { case (k, v) =>
        b += kc.structuralValue(k) -> vc.structuralValue(v)
      }
      b.result()
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: m.Map[K, V]): Boolean = false
  override def registerByteSizeObserver(
    value: m.Map[K, V],
    observer: ElementByteSizeObserver
  ): Unit = {
    lc.registerByteSizeObserver(value.size, observer)
    value.foreach { case (k, v) =>
      kc.registerByteSizeObserver(k, observer)
      vc.registerByteSizeObserver(v, observer)
    }
  }

  override def toString: String =
    s"MutableMapCoder($kc, $vc)"
}

private object SFloatCoder extends BCoder[Float] {
  private val bc = FloatCoder.of()

  override def encode(value: Float, outStream: OutputStream): Unit = bc.encode(value, outStream)
  override def decode(inStream: InputStream): Float = bc.decode(inStream)
  override def getCoderArguments: util.List[_ <: BCoder[_]] = bc.getCoderArguments
  override def verifyDeterministic(): Unit = bc.verifyDeterministic()
  override def structuralValue(value: Float): AnyRef =
    if (value.isNaN) {
      new StructuralByteArray(CoderUtils.encodeToByteArray(bc, value: java.lang.Float))
    } else {
      java.lang.Float.valueOf(value)
    }
  override def toString: String = "FloatCoder"
}

private object SDoubleCoder extends BCoder[Double] {
  private val bc = DoubleCoder.of()

  override def encode(value: Double, outStream: OutputStream): Unit = bc.encode(value, outStream)
  override def decode(inStream: InputStream): Double = bc.decode(inStream)
  override def getCoderArguments: util.List[_ <: BCoder[_]] = bc.getCoderArguments
  override def verifyDeterministic(): Unit = bc.verifyDeterministic()
  override def structuralValue(value: Double): AnyRef =
    if (value.isNaN) {
      new StructuralByteArray(CoderUtils.encodeToByteArray(bc, value: java.lang.Double))
    } else {
      java.lang.Double.valueOf(value)
    }
  override def toString: String = "DoubleCoder"
}

trait ScalaCoders {
  implicit def charCoder: Coder[Char] =
    Coder.xmap(Coder.beam(ByteCoder.of()))(_.toChar, _.toByte)
  implicit def byteCoder: Coder[Byte] =
    Coder.beam(ByteCoder.of().asInstanceOf[BCoder[Byte]])
  implicit def stringCoder: Coder[String] =
    Coder.beam(StringUtf8Coder.of())
  implicit def shortCoder: Coder[Short] =
    Coder.beam(BigEndianShortCoder.of().asInstanceOf[BCoder[Short]])
  implicit def intCoder: Coder[Int] =
    Coder.beam(VarIntCoder.of().asInstanceOf[BCoder[Int]])
  implicit def longCoder: Coder[Long] =
    Coder.beam(BigEndianLongCoder.of().asInstanceOf[BCoder[Long]])
  implicit def floatCoder: Coder[Float] = Coder.beam(SFloatCoder)
  implicit def doubleCoder: Coder[Double] = Coder.beam(SDoubleCoder)

  implicit def booleanCoder: Coder[Boolean] =
    Coder.beam(BooleanCoder.of().asInstanceOf[BCoder[Boolean]])
  implicit def unitCoder: Coder[Unit] = Coder.beam(UnitCoder)
  implicit def nothingCoder: Coder[Nothing] = Coder.beam[Nothing](NothingCoder)

  implicit def bigIntCoder: Coder[BigInt] =
    Coder.xmap(Coder.beam(BigIntegerCoder.of()))(BigInt.apply, _.bigInteger)

  implicit def bigDecimalCoder: Coder[BigDecimal] =
    Coder.xmap(Coder.beam(BigDecimalCoder.of()))(BigDecimal.apply, _.bigDecimal)

  implicit def tryCoder[A: Coder]: Coder[Try[A]] =
    Coder.gen[Try[A]]

  implicit def eitherCoder[A: Coder, B: Coder]: Coder[Either[A, B]] =
    Coder.gen[Either[A, B]]

  implicit def optionCoder[T, S[_] <: Option[_]](implicit c: Coder[T]): Coder[S[T]] =
    Coder
      .transform(c)(bc => Coder.beam(new OptionCoder[T](bc)))
      .asInstanceOf[Coder[S[T]]]

  implicit def noneCoder: Coder[None.type] =
    optionCoder[Nothing, Option](nothingCoder).asInstanceOf[Coder[None.type]]

  implicit def bitSetCoder: Coder[BitSet] = Coder.beam(new BitSetCoderInternal)

  implicit def seqCoder[T: Coder]: Coder[Seq[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new SeqCoder[T](bc)))

  // TODO: proper chunking implementation
  implicit def iterableCoder[T: Coder]: Coder[Iterable[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new IterableCoder[T](bc)))

  implicit def throwableCoder[T <: Throwable: ClassTag]: Coder[T] =
    Coder.kryo[T]

  // specialized coder. Since `::` is a case class, Magnolia would derive an incorrect one...
  implicit def listCoder[T: Coder]: Coder[List[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new ListCoder[T](bc)))

  implicit def iterableOnceCoder[T: Coder]: Coder[IterableOnce[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new IterableOnceCoder[T](bc)))

  implicit def setCoder[T: Coder]: Coder[Set[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new SetCoder[T](bc)))

  implicit def mutableSetCoder[T: Coder]: Coder[m.Set[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new MutableSetCoder[T](bc)))

  implicit def vectorCoder[T: Coder]: Coder[Vector[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new VectorCoder[T](bc)))

  implicit def arrayBufferCoder[T: Coder]: Coder[m.ArrayBuffer[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new ArrayBufferCoder[T](bc)))

  implicit def bufferCoder[T: Coder]: Coder[m.Buffer[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new BufferCoder[T](bc)))

  implicit def listBufferCoder[T: Coder]: Coder[m.ListBuffer[T]] =
    Coder.xmap(bufferCoder[T])(x => m.ListBuffer(x.toSeq: _*), identity)

  // TODO: scala3 restore previous implementation (see ArrayCoder)
  // Coder.transform(Coder[T])(bc => Coder.beam(new ArrayCoder[T](bc)))
  implicit def arrayCoder[T: Coder: ClassTag]: Coder[Array[T]] = ???

  implicit def arrayByteCoder: Coder[Array[Byte]] =
    Coder.beam(ByteArrayCoder.of())

  implicit def wrappedArrayCoder[T: Coder: ClassTag](implicit
    wrap: Array[T] => m.WrappedArray[T]
  ): Coder[m.WrappedArray[T]] =
    Coder.xmap(Coder[Array[T]])(wrap, _.toArray)

  implicit def mutableMapCoder[K: Coder, V: Coder]: Coder[m.Map[K, V]] =
    Coder.transform(Coder[K]) { kc =>
      Coder.transform(Coder[V])(vc => Coder.beam(new MutableMapCoder[K, V](kc, vc)))
    }

  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] =
    Coder.transform(Coder[K]) { kc =>
      Coder.transform(Coder[V])(vc => Coder.beam(new MapCoder[K, V](kc, vc)))
    }

  implicit def sortedSetCoder[T: Coder: Ordering]: Coder[SortedSet[T]] =
    Coder.transform(Coder[T])(bc => Coder.beam(new SortedSetCoder[T](bc)))

  // implicit def enumerationCoder[E <: Enumeration]: Coder[E#Value] = ???
}

private[coders] object ScalaCoders extends ScalaCoders
