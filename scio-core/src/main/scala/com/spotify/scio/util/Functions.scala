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
import java.util.{ArrayList => JArrayList, List => JList}

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.twitter.algebird.{Monoid, Semigroup}
import org.apache.beam.sdk.coders.{CoderRegistry, Coder => BCoder}
import org.apache.beam.sdk.transforms.Combine.{CombineFn => BCombineFn}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.Partition.PartitionFn
import org.apache.beam.sdk.transforms.{DoFn, ProcessFunction, SerializableFunction}

import scala.collection.JavaConverters._

private[scio] object Functions {

  private[this] val BufferSize = 20

  private object Fns {
    def fold[A, B](accumulator: A, list: JIterable[B])(f: (A, B) => A): A = {
      val iter = list.iterator()
      var acc: A = accumulator
      while (iter.hasNext) {
        acc = f(acc, iter.next())
      }
      acc
    }

    def reduce[A](list: JIterable[A])(op: (A, A) => A): A = {
      val iter = list.iterator()
      var acc = iter.next()
      while (iter.hasNext) {
        acc = op(acc, iter.next())
      }
      acc
    }

    def reduceOption[A](list: JIterable[A])(op: (A, A) => A): Option[A] = {
      val iter = list.iterator()
      if (iter.hasNext) {
        Some(reduce(list)(op))
      } else {
        None
      }
    }
  }

  private abstract class CombineFn[VI, VA, VO] extends BCombineFn[VI, VA, VO] with NamedFn {
    val vacoder: Coder[VA]
    val vocoder: Coder[VO]

    override def getAccumulatorCoder(registry: CoderRegistry, inputCoder: BCoder[VI]): BCoder[VA] =
      CoderMaterializer.beamWithDefault(vacoder, registry)

    override def getDefaultOutputCoder(registry: CoderRegistry,
                                       inputCoder: BCoder[VI]): BCoder[VO] =
      CoderMaterializer.beamWithDefault(vocoder, registry)
  }

  def aggregateFn[T: Coder, U: Coder](
    zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): BCombineFn[T, (U, JList[T]), U] =
    new CombineFn[T, (U, JList[T]), U] {

      val vacoder = Coder[(U, JList[T])]
      val vocoder = Coder[U]

      // defeat closure
      private[this] val s = ClosureCleaner(seqOp)
      private[this] val c = ClosureCleaner(combOp)

      private def fold(accumulator: (U, JList[T])): U = {
        val (a, l) = accumulator
        Fns.fold(a, l)(s)
      }

      override def createAccumulator(): (U, JList[T]) =
        (zeroValue, new JArrayList[T])

      override def addInput(accumulator: (U, JList[T]), input: T): (U, JList[T]) = {
        val (_, l) = accumulator
        l.add(input)
        if (l.size() >= BufferSize) {
          (fold(accumulator), new JArrayList[T]())
        } else {
          accumulator
        }
      }

      override def extractOutput(accumulator: (U, JList[T])): U = fold(accumulator)

      override def mergeAccumulators(accumulators: JIterable[(U, JList[T])]): (U, JList[T]) = {
        val empty = new JArrayList[T]()
        Fns.reduce(accumulators) { case (a, b) => (c(fold(a), fold(b)), empty) }
      }

    }

  def combineFn[T: Coder, C: Coder](
    createCombiner: T => C,
    mergeValue: (C, T) => C,
    mergeCombiners: (C, C) => C): BCombineFn[T, (Option[C], JList[T]), C] =
    new CombineFn[T, (Option[C], JList[T]), C] {

      val vacoder = Coder[(Option[C], JList[T])]
      val vocoder = Coder[C]

      // defeat closure
      private[this] val cc = ClosureCleaner(createCombiner)
      private[this] val mv = ClosureCleaner(mergeValue)
      private[this] val mc = ClosureCleaner(mergeCombiners)

      private def foldOption(accumulator: (Option[C], JList[T])): Option[C] = {
        val (opt, l) = accumulator
        if (opt.isDefined) {
          Some(Fns.fold(opt.get, l)(mv))
        } else {
          if (opt.isEmpty && l.isEmpty) {
            None
          } else {
            var c = cc(l.get(0))
            var i = 1
            while (i < l.size) {
              c = mv(c, l.get(i))
              i += 1
            }
            Some(c)
          }
        }
      }

      override def createAccumulator(): (Option[C], JList[T]) =
        (None, new JArrayList[T]())

      override def addInput(accumulator: (Option[C], JList[T]), input: T): (Option[C], JList[T]) = {
        val (_, l) = accumulator
        l.add(input)
        if (l.size() >= BufferSize) {
          (foldOption(accumulator), new JArrayList[T]())
        } else {
          accumulator
        }
      }

      override def extractOutput(accumulator: (Option[C], JList[T])): C =
        foldOption(accumulator).get

      override def mergeAccumulators(
        accumulators: JIterable[(Option[C], JList[T])]): (Option[C], JList[T]) = {
        val iter = accumulators.iterator()
        val empty = new JArrayList[T]()

        if (!iter.hasNext) {
          (None, empty)
        } else {
          Fns.reduce(accumulators) { (a, b) =>
            val aa = foldOption(a)
            val bb = foldOption(b)

            val result = if (aa.isDefined && bb.isDefined) {
              Some(mc(aa.get, bb.get))
            } else {
              if (aa.isDefined || bb.isDefined) {
                aa.orElse(bb)
              } else {
                None
              }
            }

            (result, empty)
          }
        }
      }
    }

  def flatMapFn[T, U](f: T => TraversableOnce[U]): DoFn[T, U] =
    new NamedDoFn[T, U] {
      private[this] val g = ClosureCleaner(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
        val i = g(c.element()).toIterator
        while (i.hasNext) c.output(i.next())
      }
    }

  def processFn[T, U](f: T => U): ProcessFunction[T, U] = new NamedProcessFn[T, U] {
    private[this] val g = ClosureCleaner(f) // defeat closure

    @throws[Exception]
    override def apply(input: T): U = g(input)
  }

  def serializableFn[T, U](f: T => U): SerializableFunction[T, U] =
    new NamedSerializableFn[T, U] {
      private[this] val g = ClosureCleaner(f) // defeat closure
      override def apply(input: T): U = g(input)
    }

  def mapFn[T, U](f: T => U): DoFn[T, U] = new NamedDoFn[T, U] {
    private[this] val g = ClosureCleaner(f) // defeat closure
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit =
      c.output(g(c.element()))
  }

  def partitionFn[T](numPartitions: Int, f: T => Int): PartitionFn[T] =
    new NamedPartitionFn[T] {
      private[this] val g = ClosureCleaner(f) // defeat closure
      override def partitionFor(elem: T, numPartitions: Int): Int = g(elem)
    }

  private abstract class ReduceFn[T: Coder] extends CombineFn[T, JList[T], T] {

    override def createAccumulator(): JList[T] = new JArrayList[T]()

    override def addInput(accumulator: JList[T], input: T): JList[T] = {
      accumulator.add(input)
      if (accumulator.size > BufferSize) {
        val v = reduceOption(accumulator).get
        accumulator.clear()
        accumulator.add(v)
      }
      accumulator
    }

    override def extractOutput(accumulator: JList[T]): T =
      reduceOption(accumulator).get

    override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
      val iter = accumulators.iterator()
      val partial: JArrayList[T] = new JArrayList[T]()
      while (iter.hasNext) {
        reduceOption(iter.next()).foreach(partial.add(_))
      }
      val r = new JArrayList[T]()
      reduceOption(partial).foreach(r.add)
      r
    }

    def reduceOption(accumulator: JIterable[T]): Option[T]

  }

  def reduceFn[T: Coder](f: (T, T) => T): BCombineFn[T, JList[T], T] =
    new ReduceFn[T] {
      val vacoder = Coder[JList[T]]
      val vocoder = Coder[T]
      private[this] val g = ClosureCleaner(f) // defeat closure

      override def reduceOption(accumulator: JIterable[T]): Option[T] =
        Fns.reduceOption(accumulator)(g)
    }

  def reduceFn[T: Coder](sg: Semigroup[T]): BCombineFn[T, JList[T], T] =
    new ReduceFn[T] {
      val vacoder = Coder[JList[T]]
      val vocoder = Coder[T]
      private[this] val _sg = ClosureCleaner(sg) // defeat closure

      override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
        val iter = accumulators.iterator()
        val acc: JArrayList[T] = new JArrayList[T]()
        while (iter.hasNext) {
          Fns.reduceOption(iter.next())(_sg.plus(_, _)).foreach(acc.add(_))
        }

        val combined: T = _sg.sumOption(acc.asScala).get
        val list = new JArrayList[T]()
        list.add(combined)
        list
      }

      override def reduceOption(accumulator: JIterable[T]): Option[T] =
        Fns.reduceOption(accumulator)(_sg.plus(_, _))
    }

  def reduceFn[T: Coder](mon: Monoid[T]): BCombineFn[T, JList[T], T] =
    new ReduceFn[T] {
      val vacoder = Coder[JList[T]]
      val vocoder = Coder[T]
      private[this] val _mon = ClosureCleaner(mon) // defeat closure

      override def reduceOption(accumulator: JIterable[T]): Option[T] =
        Fns.reduceOption(accumulator)(_mon.plus(_, _)).orElse(Some(_mon.zero))
    }

}
