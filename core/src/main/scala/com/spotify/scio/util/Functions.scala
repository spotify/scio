package com.spotify.scio.util

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import com.google.cloud.dataflow.sdk.coders.{Coder, CoderRegistry}
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.Partition.PartitionFn
import com.google.cloud.dataflow.sdk.transforms.{DoFn, SerializableFunction}
import com.google.common.collect.Lists
import com.spotify.scio.coders.KryoAtomicCoder
import com.twitter.algebird.{Monoid, Semigroup}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[scio] object Functions {

  private val BUFFER_SIZE = 20

  private abstract class KryoCombineFn[VI, VA, VO] extends CombineFn[VI, VA, VO] {

    override def getAccumulatorCoder(registry: CoderRegistry, inputCoder: Coder[VI]): Coder[VA] =
      KryoAtomicCoder[VA]

    override def getDefaultOutputCoder(registry: CoderRegistry, inputCoder: Coder[VI]): Coder[VO] =
      KryoAtomicCoder[VO]

  }

  def aggregateFn[T, U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
  : CombineFn[T, (U, JList[T]), U] = new KryoCombineFn[T, (U, JList[T]), U] {

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

    override def extractOutput(accumulator: (U, JList[T])): U = accumulator._2.asScala.foldLeft(accumulator._1)(s)

    override def mergeAccumulators(accumulators: JIterable[(U, JList[T])]): (U, JList[T]) = {
      val combined = accumulators.asScala.map(fold).reduce(c)
      (combined, Lists.newArrayList())
    }

  }

  def combineFn[T, C: ClassTag](createCombiner: T => C, mergeValue: (C, T) => C, mergeCombiners: (C, C) => C)
  : CombineFn[T, (Option[C], JList[T]), C] = new KryoCombineFn[T, (Option[C], JList[T]), C] {

    // defeat closure
    val cc = ClosureCleaner(createCombiner)
    val mv = ClosureCleaner(mergeValue)
    val mc = ClosureCleaner(mergeCombiners)

    private def fold(accumulator: (Option[C], JList[T])): C = {
      val (a, l) = accumulator
      if (a.isDefined) {
        l.asScala.foldLeft(a.get)(mv)
      } else {
        var c = cc(l.get(0))
        var i = 1
        while (i < l.size) {
          c = mv(c, l.get(i))
          i += 1
        }
        c
      }
    }

    override def createAccumulator(): (Option[C], JList[T]) = (None, Lists.newArrayList())

    override def addInput(accumulator: (Option[C], JList[T]), input: T): (Option[C], JList[T]) = {
      val (a, l) = accumulator
      l.add(input)
      if (l.size() >= BUFFER_SIZE) {
        (Some(fold(accumulator)), Lists.newArrayList())
      } else {
        accumulator
      }
    }

    // TODO: maybe unsafe if addInput is never called?
    override def extractOutput(accumulator: (Option[C], JList[T])): C = fold(accumulator)


    override def mergeAccumulators(accumulators: JIterable[(Option[C], JList[T])]): (Option[C], JList[T]) = {
      val combined = accumulators.asScala.map(fold).reduce(mc)
      (Some(combined), Lists.newArrayList())
    }

  }

  def flatMapFn[T, U](f: T => TraversableOnce[U]): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = g(c.element()).foreach(c.output)
  }

  def serializableFn[T, U](f: T => U): SerializableFunction[T, U] = new SerializableFunction[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def apply(input: T): U = g(input)
  }

  def mapFn[T, U](f: T => U): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element()))
  }

  def partitionFn[T](numPartitions: Int, f: T => Int): PartitionFn[T] = new PartitionFn[T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def partitionFor(elem: T, numPartitions: Int): Int = f(elem)
  }

  private abstract class ReduceFn[T] extends KryoCombineFn[T, JList[T], T] {

    override def createAccumulator(): JList[T] = Lists.newArrayList()

    override def addInput(accumulator: JList[T], input: T): JList[T] = {
      accumulator.add(input)
      if (accumulator.size > BUFFER_SIZE) {
        val combined = reduce(accumulator.asScala)
        accumulator.clear()
        accumulator.add(combined)
      }
      accumulator
    }

    override def extractOutput(accumulator: JList[T]): T = reduce(accumulator.asScala)

    override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
      val partial: Iterable[T] = accumulators.asScala.map(a => reduce(a.asScala))
      val combined = reduce(partial)
      Lists.newArrayList(combined)
    }

    def reduce(accumulator: Iterable[T]): T

  }

  def reduceFn[T](f: (T, T) => T): CombineFn[T, JList[T], T] = new ReduceFn[T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def reduce(accumulator: Iterable[T]): T = accumulator.reduce(g)
  }

  def reduceFn[T](sg: Semigroup[T]): CombineFn[T, JList[T], T] = new ReduceFn[T] {
    val _sg = ClosureCleaner(sg)  // defeat closure
    override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
      val partial = accumulators.asScala.flatMap(a => _sg.sumOption(a.asScala))
      val combined = _sg.sumOption(partial).get
      Lists.newArrayList(combined)
    }
    override def reduce(accumulator: Iterable[T]): T = _sg.sumOption(accumulator).get
  }

  def reduceFn[T](mon: Monoid[T]): CombineFn[T, JList[T], T] = new ReduceFn[T] {
    val _mon = ClosureCleaner(mon)  // defeat closure
    override def reduce(accumulator: Iterable[T]): T = _mon.sum(accumulator)
  }

}
