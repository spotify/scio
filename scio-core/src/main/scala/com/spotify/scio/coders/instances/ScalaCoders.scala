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

import com.spotify.scio.coders.{Coder, CoderDerivation, CoderGrammar}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder, _}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.apache.beam.sdk.util.{BufferedElementCountingOutputStream, CoderUtils, VarInt}

import java.io.{InputStream, OutputStream}
import java.util.{Collections, List => JList}
import scala.collection.compat._
import scala.collection.immutable.SortedMap
import scala.collection.{mutable => m, AbstractIterable, BitSet, SortedSet}
import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

private[coders] object UnitCoder extends AtomicCoder[Unit] {
  override def encode(value: Unit, os: OutputStream): Unit = ()
  override def decode(is: InputStream): Unit = ()

  override def consistentWithEquals(): Boolean = true
  override def isRegisterByteSizeObserverCheap(value: Unit): Boolean = true
  override def getEncodedElementByteSize(value: Unit): Long = 0
  override def toString: String = "UnitCoder"
}

private object NothingCoder extends AtomicCoder[Nothing] {
  override def encode(value: Nothing, os: OutputStream): Unit = ()
  override def decode(is: InputStream): Nothing =
    // can't possibly happen
    throw new IllegalStateException("Trying to decode a value of type Nothing is impossible")
  override def consistentWithEquals(): Boolean = true
  override def isRegisterByteSizeObserverCheap(value: Nothing): Boolean = true
  override def getEncodedElementByteSize(value: Nothing): Long = 0
  override def toString: String = "NothingCoder"
}

abstract private[coders] class BaseSeqLikeCoder[M[_], T](val elemCoder: BCoder[T])
    extends StructuredCoder[M[T]] {
  override def getCoderArguments: JList[_ <: BCoder[_]] =
    Collections.singletonList(elemCoder)

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit = elemCoder.verifyDeterministic()
  override def consistentWithEquals(): Boolean = elemCoder.consistentWithEquals()
  override def toString: String = s"${getClass.getSimpleName}($elemCoder)"
}

abstract private[coders] class SeqLikeCoder[M[_], T](bc: BCoder[T])(implicit
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

  override def registerByteSizeObserver(value: M[T], observer: ElementByteSizeObserver): Unit = {
    val traversable = ev(value)
    observer.update(VarInt.getLength(traversable.iterator.size))
    traversable.iterator.foreach(v => elemCoder.registerByteSizeObserver(v, observer))
  }
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

  override def registerByteSizeObserver(value: M[T], observer: ElementByteSizeObserver): Unit = {
    val traversable = ev(value)
    val size = traversable.iterator.foldLeft(0) { (count, v) =>
      elemCoder.registerByteSizeObserver(v, observer)
      count + 1
    }
    if (size > 0) {
      // this is an approximation, but it's better than nothing
      // BufferedElementCountingOutputStream can split the iterator in many chunks
      observer.update(VarInt.getLength(size))
    }
    observer.update(1L) // terminator byte
  }
}

// keep this for binary compatibility
@deprecated("Use Coder.gen[Option[T]] instead", "0.14.0")
private[coders] class OptionCoder[T](bc: BCoder[T]) extends StructuredCoder[Option[T]] {
  private[this] val bcoder = BooleanCoder.of()

  override def encode(value: Option[T], os: OutputStream): Unit = {
    bcoder.encode(value.isDefined, os)
    value.foreach(bc.encode(_, os))
  }

  override def decode(is: InputStream): Option[T] = {
    val isDefined = bcoder.decode(is)
    if (isDefined) Some(bc.decode(is)) else None
  }

  override def getCoderArguments: JList[_ <: BCoder[_]] =
    Collections.singletonList(bc)

  override def verifyDeterministic(): Unit = bc.verifyDeterministic()

  override def consistentWithEquals(): Boolean = bc.consistentWithEquals()

  override def toString: String = s"OptionCoder($bc)"
}

private[coders] class SeqCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Seq, T](bc) {
  override def decode(inStream: InputStream): Seq[T] = decode(inStream, Seq.newBuilder[T])
}

private[coders] class ListCoder[T](bc: BCoder[T]) extends SeqLikeCoder[List, T](bc) {
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

private[coders] class VectorCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Vector, T](bc) {
  override def decode(inStream: InputStream): Vector[T] = decode(inStream, Vector.newBuilder[T])
}

private[coders] class ArrayCoder[
  @specialized(Short, Int, Long, Float, Double, Boolean, Char) T: ClassTag
](
  bc: BCoder[T]
) extends SeqLikeCoder[Array, T](bc) {
  override def decode(inStream: InputStream): Array[T] = {
    val size = VarInt.decodeInt(inStream)
    val arr = new Array[T](size)
    var i = 0
    while (i < size) {
      arr(i) = bc.decode(inStream)
      i += 1
    }
    arr
  }
  override def consistentWithEquals(): Boolean = false
}

private class ArrayBufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.ArrayBuffer, T](bc) {
  override def decode(inStream: InputStream): m.ArrayBuffer[T] =
    decode(inStream, m.ArrayBuffer.newBuilder[T])
}

private[coders] class BufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.Buffer, T](bc) {
  override def decode(inStream: InputStream): m.Buffer[T] = decode(inStream, m.Buffer.newBuilder[T])
}

private[coders] class SetCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Set, T](bc) {
  override def decode(inStream: InputStream): Set[T] = decode(inStream, Set.newBuilder[T])

  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of entries in a Set may be non-deterministic."
    )
}

private[coders] class MutableSetCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.Set, T](bc) {
  override def decode(inStream: InputStream): m.Set[T] = decode(inStream, m.Set.newBuilder[T])
}

private class SortedSetCoder[T: Ordering](bc: BCoder[T]) extends SeqLikeCoder[SortedSet, T](bc) {
  override def decode(inStream: InputStream): SortedSet[T] =
    decode(inStream, SortedSet.newBuilder[T])
}

private[coders] class MutablePriorityQueueCoder[T: Ordering](bc: BCoder[T])
    extends SeqLikeCoder[m.PriorityQueue, T](bc) {
  override def consistentWithEquals(): Boolean = false // PriorityQueue does not define equality
  override def decode(inStream: InputStream): m.PriorityQueue[T] =
    decode(inStream, m.PriorityQueue.newBuilder[T])
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of elements in a priority queue may be non-deterministic."
    )
}

private[coders] class BitSetCoder extends AtomicCoder[BitSet] {

  def decode(in: InputStream): BitSet = {
    val l = VarInt.decodeInt(in)
    val builder = BitSet.newBuilder
    builder.sizeHint(l)
    (1 to l).foreach(_ => builder += VarInt.decodeInt(in))

    builder.result()
  }

  def encode(ts: BitSet, out: OutputStream): Unit = {
    VarInt.encode(ts.size, out)
    ts.foreach(v => VarInt.encode(v, out))
  }

  override def consistentWithEquals(): Boolean = true

  override def toString: String = "BitSetCoder"
}

abstract private[coders] class MapLikeCoder[K, V, M[_, _]](
  val keyCoder: BCoder[K],
  val valueCoder: BCoder[V]
)(implicit
  ev: M[K, V] => IterableOnce[(K, V)]
) extends StructuredCoder[M[K, V]] {
  override def getCoderArguments: JList[_ <: BCoder[_]] = List(keyCoder, valueCoder).asJava

  override def encode(value: M[K, V], os: OutputStream): Unit = {
    val traversable = ev(value)
    VarInt.encode(traversable.iterator.size, os)
    traversable.iterator.foreach { case (k, v) =>
      keyCoder.encode(k, os)
      valueCoder.encode(v, os)
    }
  }

  def decode(is: InputStream, builder: m.Builder[(K, V), M[K, V]]): M[K, V] = {
    val size = VarInt.decodeInt(is)
    builder.sizeHint(size)
    var i = 0
    while (i < size) {
      val k = keyCoder.decode(is)
      val v = valueCoder.decode(is)
      builder += (k -> v)
      i = i + 1
    }
    builder.result()
  }

  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of entries in a Map may be non-deterministic."
    )

  override def consistentWithEquals(): Boolean =
    keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals()

  override def structuralValue(value: M[K, V]): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      val b = Map.newBuilder[Any, Any]
      val traversable = ev(value)
      b.sizeHint(traversable.iterator.size)
      traversable.iterator.foreach { case (k, v) =>
        b += keyCoder.structuralValue(k) -> valueCoder.structuralValue(v)
      }
      b.result()
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: M[K, V]): Boolean = false
  override def registerByteSizeObserver(
    value: M[K, V],
    observer: ElementByteSizeObserver
  ): Unit = {
    val traversable = ev(value)
    observer.update(VarInt.getLength(traversable.iterator.size))
    traversable.iterator.foreach { case (k, v) =>
      keyCoder.registerByteSizeObserver(k, observer)
      valueCoder.registerByteSizeObserver(v, observer)
    }
  }

  override def toString: String = s"${getClass.getSimpleName}($keyCoder, $valueCoder)"
}

private[coders] class MapCoder[K, V](kc: BCoder[K], vc: BCoder[V])
    extends MapLikeCoder[K, V, Map](kc, vc) {
  override def decode(is: InputStream): Map[K, V] =
    decode(is, Map.newBuilder[K, V])
}

private class MutableMapCoder[K, V](kc: BCoder[K], vc: BCoder[V])
    extends MapLikeCoder[K, V, m.Map](kc, vc) {

  override def decode(inStream: InputStream): m.Map[K, V] =
    decode(inStream, m.Map.newBuilder[K, V])
}

private[coders] class SortedMapCoder[K: Ordering, V](kc: BCoder[K], vc: BCoder[V])
    extends MapLikeCoder[K, V, SortedMap](kc, vc) {
  override def decode(is: InputStream): SortedMap[K, V] =
    decode(is, SortedMap.newBuilder[K, V])
  override def verifyDeterministic(): Unit = {
    keyCoder.verifyDeterministic()
    valueCoder.verifyDeterministic()
  }
}

private[coders] object SFloatCoder extends BCoder[Float] {
  private val bc = FloatCoder.of()

  override def encode(value: Float, outStream: OutputStream): Unit = bc.encode(value, outStream)
  override def decode(inStream: InputStream): Float = bc.decode(inStream)
  override def getCoderArguments: JList[_ <: BCoder[_]] = bc.getCoderArguments
  override def verifyDeterministic(): Unit = bc.verifyDeterministic()
  override def structuralValue(value: Float): AnyRef =
    if (value.isNaN) {
      new StructuralByteArray(CoderUtils.encodeToByteArray(bc, value: java.lang.Float))
    } else {
      java.lang.Float.valueOf(value)
    }
  override def consistentWithEquals(): Boolean = bc.consistentWithEquals()
  override def toString: String = "FloatCoder"
}

private[coders] object SDoubleCoder extends BCoder[Double] {
  private val bc = DoubleCoder.of()

  override def encode(value: Double, outStream: OutputStream): Unit = bc.encode(value, outStream)
  override def decode(inStream: InputStream): Double = bc.decode(inStream)
  override def getCoderArguments: JList[_ <: BCoder[_]] = bc.getCoderArguments
  override def verifyDeterministic(): Unit = bc.verifyDeterministic()
  override def structuralValue(value: Double): AnyRef =
    if (value.isNaN) {
      new StructuralByteArray(CoderUtils.encodeToByteArray(bc, value: java.lang.Double))
    } else {
      java.lang.Double.valueOf(value)
    }
  override def consistentWithEquals(): Boolean = bc.consistentWithEquals()
  override def toString: String = "DoubleCoder"
}

trait ScalaCoders extends CoderGrammar with CoderDerivation {
  implicit lazy val charCoder: Coder[Char] =
    xmap(beam(ByteCoder.of()))(_.toChar, _.toByte)
  implicit lazy val byteCoder: Coder[Byte] =
    beam(ByteCoder.of().asInstanceOf[BCoder[Byte]])
  implicit lazy val stringCoder: Coder[String] =
    beam(StringUtf8Coder.of())
  implicit lazy val shortCoder: Coder[Short] =
    beam(BigEndianShortCoder.of().asInstanceOf[BCoder[Short]])
  implicit lazy val intCoder: Coder[Int] =
    beam(VarIntCoder.of().asInstanceOf[BCoder[Int]])
  implicit lazy val longCoder: Coder[Long] =
    beam(BigEndianLongCoder.of().asInstanceOf[BCoder[Long]])
  implicit lazy val floatCoder: Coder[Float] = beam(SFloatCoder)
  implicit lazy val doubleCoder: Coder[Double] = beam(SDoubleCoder)
  implicit lazy val booleanCoder: Coder[Boolean] = beam(
    BooleanCoder.of().asInstanceOf[BCoder[Boolean]]
  )
  implicit lazy val unitCoder: Coder[Unit] = beam[Unit](UnitCoder)
  implicit lazy val nothingCoder: Coder[Nothing] = beam[Nothing](NothingCoder)
  implicit lazy val bigIntCoder: Coder[BigInt] =
    xmap(beam(BigIntegerCoder.of()))(BigInt.apply, _.bigInteger)
  implicit lazy val bigDecimalCoder: Coder[BigDecimal] =
    xmap(beam(BigDecimalCoder.of()))(BigDecimal.apply, _.bigDecimal)
  implicit lazy val bitSetCoder: Coder[BitSet] = beam(new BitSetCoder)

  implicit def enumerationCoder[E <: Enumeration: ClassTag]: Coder[E#Value] = {
    import scala.reflect.runtime.{currentMirror => m}
    val sym = m.moduleSymbol(classTag[E].runtimeClass)
    val e = m.reflectModule(sym).instance.asInstanceOf[E]
    xmap(Coder[Int])(e.apply, _.id)
  }

  implicit def throwableCoder[T <: Throwable: ClassTag]: Coder[T] = kryo[T]
  implicit def seqCoder[T: Coder]: Coder[Seq[T]] =
    transform(Coder[T])(bc => beam(new SeqCoder[T](bc)))

  // TODO: proper chunking implementation
  implicit def iterableCoder[T: Coder]: Coder[Iterable[T]] =
    transform(Coder[T])(bc => beam(new IterableCoder[T](bc)))

  // specialized coder. Since `::` is a case class, Magnolia would derive an incorrect one...
  implicit def listCoder[T: Coder]: Coder[List[T]] =
    transform(Coder[T])(bc => beam(new ListCoder[T](bc)))
  implicit def iterableOnceCoder[T: Coder]: Coder[IterableOnce[T]] =
    transform(Coder[T])(bc => beam(new IterableOnceCoder[T](bc)))
  implicit def setCoder[T: Coder]: Coder[Set[T]] =
    transform(Coder[T])(bc => beam(new SetCoder[T](bc)))

  implicit def mutableSetCoder[T: Coder]: Coder[m.Set[T]] =
    transform(Coder[T])(bc => beam(new MutableSetCoder[T](bc)))

  implicit def vectorCoder[T: Coder]: Coder[Vector[T]] =
    transform(Coder[T])(bc => beam(new VectorCoder[T](bc)))

  implicit def arrayBufferCoder[T: Coder]: Coder[m.ArrayBuffer[T]] =
    transform(Coder[T])(bc => beam(new ArrayBufferCoder[T](bc)))

  implicit def bufferCoder[T: Coder]: Coder[m.Buffer[T]] =
    transform(Coder[T])(bc => beam(new BufferCoder[T](bc)))

  implicit def listBufferCoder[T: Coder]: Coder[m.ListBuffer[T]] =
    xmap(bufferCoder[T])(x => m.ListBuffer(x.toSeq: _*), identity)

  implicit def arrayCoder[T: Coder: ClassTag]: Coder[Array[T]] =
    transform(Coder[T])(bc => beam(new ArrayCoder[T](bc)))

  implicit def arrayByteCoder: Coder[Array[Byte]] =
    beam(ByteArrayCoder.of())

  implicit def wrappedArrayCoder[T: Coder: ClassTag](implicit
    wrap: Array[T] => m.WrappedArray[T]
  ): Coder[m.WrappedArray[T]] =
    xmap(Coder[Array[T]])(wrap, _.toArray)

  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] =
    transform(Coder[K]) { kc =>
      transform(Coder[V])(vc => beam(new MapCoder[K, V](kc, vc)))
    }

  implicit def mutableMapCoder[K: Coder, V: Coder]: Coder[m.Map[K, V]] =
    transform(Coder[K]) { kc =>
      transform(Coder[V])(vc => beam(new MutableMapCoder[K, V](kc, vc)))
    }

  implicit def sortedMapCoder[K: Coder: Ordering, V: Coder]: Coder[SortedMap[K, V]] =
    transform(Coder[K]) { kc =>
      transform(Coder[V])(vc => beam(new SortedMapCoder[K, V](kc, vc)))
    }

  implicit def sortedSetCoder[T: Coder: Ordering]: Coder[SortedSet[T]] =
    transform(Coder[T])(bc => beam(new SortedSetCoder[T](bc)))

  implicit def priorityQueueCoder[T: Coder: Ordering]: Coder[m.PriorityQueue[T]] =
    transform(Coder[T])(bc => beam(new MutablePriorityQueueCoder[T](bc)))

  // cache common scala types to avoid macro expansions
  implicit def tryCoder[T: Coder]: Coder[Try[T]] =
    gen[Try[T]]
  implicit def successCoder[T: Coder]: Coder[Success[T]] =
    gen[Success[T]]
  implicit def failureCoder[T]: Coder[Failure[T]] =
    gen[Failure[T]]
  implicit def eitherCoder[A: Coder, B: Coder]: Coder[Either[A, B]] =
    gen[Either[A, B]]
  implicit def leftCoder[A: Coder, B]: Coder[Left[A, B]] =
    gen[Left[A, B]]
  implicit def rightCoder[A, B: Coder]: Coder[Right[A, B]] =
    gen[Right[A, B]]
  implicit def optionCoder[T: Coder]: Coder[Option[T]] =
    gen[Option[T]]
  implicit def someCoder[T: Coder]: Coder[Some[T]] =
    gen[Some[T]]
  implicit lazy val noneCoder: Coder[None.type] =
    gen[None.type]
}
private[coders] object ScalaCoders extends ScalaCoders
