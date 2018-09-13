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

package com.spotify.scio.util

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import com.google.common.collect.Lists
import com.spotify.scio.coders.{Coder, CoderMaterializer}

import org.apache.beam.sdk.coders.{CoderRegistry, Coder => BCoder}
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.Partition.PartitionFn
import org.apache.beam.sdk.transforms.{DoFn, SerializableFunction}
import com.twitter.algebird.{Monoid, Semigroup}

import scala.collection.JavaConverters._

private[scio] object Functions {

  private val BUFFER_SIZE = 20

  // TODO: rename
  private abstract class KryoCombineFn[VI, VA, VO] extends CombineFn[VI, VA, VO] with NamedFn {
    // TODO: should those coder be transient ?
    val vacoder: Coder[VA]
    val vocoder: Coder[VO]

    override def getAccumulatorCoder(registry: CoderRegistry, inputCoder: BCoder[VI]): BCoder[VA] =
      CoderMaterializer.beamWithDefault(vacoder, registry)

    override def getDefaultOutputCoder(
      registry: CoderRegistry, inputCoder: BCoder[VI]): BCoder[VO] =
      CoderMaterializer.beamWithDefault(vocoder, registry)
  }

  def aggregateFn[T: Coder, U: Coder](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
  : CombineFn[T, (U, JList[T]), U] = new KryoCombineFn[T, (U, JList[T]), U] {

    val vacoder = Coder[(U, JList[T])]
    val vocoder = Coder[U]

    // defeat closure
    val s = ClosureCleaner(seqOp)
    val c = ClosureCleaner(combOp)

    private def fold(accumulator: (U, JList[T])): U = {
      val (a, l) = accumulator
      l.asScala.foldLeft(a)(s)
    }

    override def createAccumulator(): (U, JList[T]) = (zeroValue, Lists.newArrayList())

    override def addInput(accumulator: (U, JList[T]), input: T): (U, JList[T]) = {
      val (a, l) = accumulator
      l.add(input)
      if (l.size() >= BUFFER_SIZE) {
        (fold(accumulator), Lists.newArrayList())
      } else {
        accumulator
      }
    }

    override def extractOutput(accumulator: (U, JList[T])): U =
      accumulator._2.asScala.foldLeft(accumulator._1)(s)

    override def mergeAccumulators(accumulators: JIterable[(U, JList[T])]): (U, JList[T]) = {
      val combined = accumulators.iterator.asScala.map(fold).reduce(c)
      (combined, Lists.newArrayList())
    }

  }

  def combineFn[T: Coder, C: Coder](createCombiner: T => C,
                                mergeValue: (C, T) => C,
                                mergeCombiners: (C, C) => C)
  : CombineFn[T, (Option[C], JList[T]), C] = new KryoCombineFn[T, (Option[C], JList[T]), C] {

    val vacoder = Coder[(Option[C], JList[T])]
    val vocoder = Coder[C]

    // defeat closure
    val cc = ClosureCleaner(createCombiner)
    val mv = ClosureCleaner(mergeValue)
    val mc = ClosureCleaner(mergeCombiners)

    private def foldOption(accumulator: (Option[C], JList[T])): Option[C] = accumulator match {
      case (Some(a), l) => Some(l.asScala.foldLeft(a)(mv))
      case (None, l) if l.isEmpty => None
      case (None, l) =>
        var c = cc(l.get(0))
        var i = 1
        while (i < l.size) {
          c = mv(c, l.get(i))
          i += 1
        }
        Some(c)
    }

    override def createAccumulator(): (Option[C], JList[T]) = (None, Lists.newArrayList())

    override def addInput(accumulator: (Option[C], JList[T]), input: T): (Option[C], JList[T]) = {
      val (a, l) = accumulator
      l.add(input)
      if (l.size() >= BUFFER_SIZE) {
        (foldOption(accumulator), Lists.newArrayList())
      } else {
        accumulator
      }
    }

    override def extractOutput(accumulator: (Option[C], JList[T])): C = foldOption(accumulator).get

    override def mergeAccumulators(accumulators: JIterable[(Option[C], JList[T])])
    : (Option[C], JList[T]) = {
      val combined = accumulators.iterator.asScala.flatMap(foldOption).reduce(mc)
      (Some(combined), Lists.newArrayList())
    }

  }

  def flatMapFn[T, U](f: T => TraversableOnce[U]): DoFn[T, U] = new NamedDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val i = g(c.element()).toIterator
      while (i.hasNext) c.output(i.next())
    }
  }

  def serializableFn[T, U](f: T => U): SerializableFunction[T, U] = new NamedSerializableFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def apply(input: T): U = g(input)
  }

  def mapFn[T, U](f: T => U): DoFn[T, U] = new NamedDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element()))
  }

  def partitionFn[T](numPartitions: Int, f: T => Int): PartitionFn[T] = new NamedPartitionFn[T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def partitionFor(elem: T, numPartitions: Int): Int = g(elem)
  }

  private abstract class ReduceFn[T: Coder] extends KryoCombineFn[T, JList[T], T] {

    override def createAccumulator(): JList[T] = Lists.newArrayList()

    override def addInput(accumulator: JList[T], input: T): JList[T] = {
      accumulator.add(input)
      if (accumulator.size > BUFFER_SIZE) {
        val v = reduceOption(accumulator.asScala).get
        accumulator.clear()
        accumulator.add(v)
      }
      accumulator
    }

    override def extractOutput(accumulator: JList[T]): T = reduceOption(accumulator.asScala).get

    override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
      val partial: Iterable[T] = accumulators.asScala.flatMap(a => reduceOption(a.asScala))
      val r = Lists.newArrayList[T]()
      reduceOption(partial).foreach(r.add)
      r
    }

    def reduceOption(accumulator: Iterable[T]): Option[T]

  }

  def reduceFn[T: Coder](f: (T, T) => T): CombineFn[T, JList[T], T] = new ReduceFn[T] {
    val vacoder = Coder[JList[T]]
    val vocoder = Coder[T]
    val g = ClosureCleaner(f)  // defeat closure
    override def reduceOption(accumulator: Iterable[T]): Option[T] = accumulator.reduceOption(g)
  }

  def reduceFn[T: Coder](sg: Semigroup[T]): CombineFn[T, JList[T], T] = new ReduceFn[T] {
    val vacoder = Coder[JList[T]]
    val vocoder = Coder[T]
    val _sg = ClosureCleaner(sg)  // defeat closure
    override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
      val partial = accumulators.asScala.flatMap(a => _sg.sumOption(a.asScala))
      val combined = _sg.sumOption(partial).get
      Lists.newArrayList(combined)
    }
    override def reduceOption(accumulator: Iterable[T]): Option[T] = _sg.sumOption(accumulator)
  }

  def reduceFn[T: Coder](mon: Monoid[T]): CombineFn[T, JList[T], T] = new ReduceFn[T] {
    val vacoder = Coder[JList[T]]
    val vocoder = Coder[T]
    val _mon = ClosureCleaner(mon)  // defeat closure
    override def reduceOption(accumulator: Iterable[T]): Option[T] =
      _mon.sumOption(accumulator).orElse(Some(_mon.zero))
  }

}
