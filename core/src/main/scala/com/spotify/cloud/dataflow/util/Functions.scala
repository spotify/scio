package com.spotify.cloud.dataflow.util

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import com.google.cloud.dataflow.sdk.coders.{Coder, CoderRegistry}
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.{SerializableFunction, DoFn}
import com.google.cloud.dataflow.sdk.transforms.Partition.PartitionFn
import com.google.common.collect.Lists
import com.spotify.cloud.dataflow.coders.KryoAtomicCoder
import com.twitter.algebird.Semigroup

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[dataflow] object Functions {

  private val BUFFER_SIZE = 20

  private abstract class KryoCombineFn[VI, VA, VO] extends CombineFn[VI, VA, VO] {

    override def getAccumulatorCoder(registry: CoderRegistry, inputCoder: Coder[VI]): Coder[VA] =
      new KryoAtomicCoder().asInstanceOf[Coder[VA]]

    override def getDefaultOutputCoder(registry: CoderRegistry, inputCoder: Coder[VI]): Coder[VO] =
      new KryoAtomicCoder().asInstanceOf[Coder[VO]]

  }

  def aggregateFn[T, U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
      : CombineFn[T, Array[U], U] = new KryoCombineFn[T, Array[U], U] {

    // defeat closure
    val s = seqOp
    val c = combOp

    override def createAccumulator(): Array[U] = Array(zeroValue)

    override def addInput(accumulator: Array[U], input: T): Array[U] = {
      accumulator(0) = s(accumulator(0), input)
      accumulator
    }

    override def extractOutput(accumulator: Array[U]): U = accumulator(0)

    override def mergeAccumulators(accumulators: JIterable[Array[U]]): Array[U] =
      Array(accumulators.asScala.map(_(0)).reduce(c))

  }

  def combineFn[T, C: ClassTag](createCombiner: T => C, mergeValue: (C, T) => C, mergeCombiners: (C, C) => C)
      : CombineFn[T, JList[C], C] = new KryoCombineFn[T, JList[C], C] {

    // defeat closure
    val cc = createCombiner
    val mv = mergeValue
    val mc = mergeCombiners

    override def createAccumulator(): JList[C] = Lists.newArrayList()


    override def addInput(accumulator: JList[C], input: T): JList[C] = {
      if (accumulator.isEmpty) {
        accumulator.add(cc(input))
      } else {
        accumulator.set(0, mv(accumulator.get(0), input))
      }
      accumulator
    }

    // TODO: maybe unsafe if addInput is never called?
    override def extractOutput(accumulator: JList[C]): C = accumulator.get(0)

    override def mergeAccumulators(accumulators: JIterable[JList[C]]): JList[C] =
      Lists.newArrayList(accumulators.asScala.map(_.get(0)).reduce(mc))

  }

  def flatMapFn[T, U](f: T => TraversableOnce[U]): DoFn[T, U] = new DoFn[T, U] {
    val g = f  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = g(c.element()).foreach(c.output)
  }

  def serializableFn[T, U](f: T => U): SerializableFunction[T, U] = new SerializableFunction[T, U] {
    val g = f  // defeat closure
    override def apply(input: T): U = g(input)
  }

  def mapFn[T, U](f: T => U): DoFn[T, U] = new DoFn[T, U] {
    val g = f  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element()))
  }
  
  def partitionFn[T](numPartitions: Int, f: T => Int): PartitionFn[T] = new PartitionFn[T] {
    val g = f  // defeat closure
    override def partitionFor(elem: T, numPartitions: Int): Int = f(elem)
  }

  def reduceFn[T](f: (T, T) => T): CombineFn[T, JList[T], T] = new KryoCombineFn[T, JList[T], T] {

    val g = f  // defeat closure

    override def createAccumulator(): JList[T] = Lists.newArrayList()

    override def addInput(accumulator: JList[T], input: T): JList[T] = {
      accumulator.add(input)
      if (accumulator.size > BUFFER_SIZE) {
        val combined = accumulator.asScala.reduce(g)
        accumulator.clear()
        accumulator.add(combined)
      }
      accumulator
    }

    // TODO: maybe unsafe if addInput is never called?
    override def extractOutput(accumulator: JList[T]): T = accumulator.asScala.reduce(g)

    override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
      val combined = accumulators.asScala.map(_.asScala.reduce(g)).reduce(g)
      Lists.newArrayList(combined)
    }

  }

  def reduceFn[T](sg: Semigroup[T]): CombineFn[T, JList[T], T] = new KryoCombineFn[T, JList[T], T] {

    val _sg = sg  // defeat closure

    override def createAccumulator(): JList[T] = Lists.newArrayList()

    override def addInput(accumulator: JList[T], input: T): JList[T] = {
      accumulator.add(input)
      if (accumulator.size > BUFFER_SIZE) {
        val opt = _sg.sumOption(accumulator.asScala)
        accumulator.clear()
        opt.foreach(accumulator.add)
      }
      accumulator
    }

    // TODO: maybe unsafe if addInput is never called?
    override def extractOutput(accumulator: JList[T]): T = _sg.sumOption(accumulator.asScala).get

    override def mergeAccumulators(accumulators: JIterable[JList[T]]): JList[T] = {
      val partial = accumulators.asScala.flatMap(a => _sg.sumOption(a.asScala))
      val combined = _sg.sumOption(partial).get
      Lists.newArrayList(combined)
    }

  }

}
