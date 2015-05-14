package com.spotify.cloud.dataflow.util

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.Max.{MaxDoubleFn, MaxLongFn, MaxIntegerFn}
import com.google.cloud.dataflow.sdk.transforms.Min.{MinDoubleFn, MinLongFn, MinIntegerFn}
import com.google.cloud.dataflow.sdk.transforms.Sum.{SumDoubleFn, SumIntegerFn, SumLongFn}
import com.google.cloud.dataflow.sdk.transforms.{Combine, Aggregator, DoFn}

import scala.collection.mutable.{Map => MMap}

sealed trait AccumulatorType[T] {
  type CF = CombineFn[T, Array[T], T]
  type BCF = Combine.BinaryCombineFn[T]

  protected def sumFnImpl(): CombineFn[_, _, _]
  protected def minFnImpl(): Combine.BinaryCombineFn[_]
  protected def maxFnImpl(): Combine.BinaryCombineFn[_]

  def sumFn(): CF = sumFnImpl().asInstanceOf[CF]
  def minFn(): BCF = minFnImpl().asInstanceOf[BCF]
  def maxFn(): BCF = maxFnImpl().asInstanceOf[BCF]
}

private[dataflow] class IntAccumulatorType extends AccumulatorType[Int] {
  override protected def sumFnImpl() = new SumIntegerFn()
  override protected def minFnImpl() = new MinIntegerFn()
  override protected def maxFnImpl() = new MaxIntegerFn()
}

private[dataflow] class LongAccumulatorType extends AccumulatorType[Long] {
  override protected def sumFnImpl() = new SumLongFn()
  override protected def minFnImpl() = new MinLongFn()
  override protected def maxFnImpl() = new MaxLongFn()
}

private[dataflow] class DoubleAccumulatorType extends AccumulatorType[Double] {
  override protected def sumFnImpl() =  new SumDoubleFn()
  override protected def minFnImpl() = new MinDoubleFn()
  override protected def maxFnImpl() = new MaxDoubleFn()
}

class Accumulator[T, U] {

  private val m: MMap[String, Aggregator[_]] = MMap.empty
  private var c: DoFn[T, U]#ProcessContext = null

  private[util] def withContext(c: DoFn[T, U]#ProcessContext): Accumulator[T, U] = {
    this.c = c
    this
  }

  private def aggregator[V, VA](name: String, f: CombineFn[V, VA, V]): Aggregator[V] =
    m.getOrElseUpdate(name, c.createAggregator(name, f)).asInstanceOf[Aggregator[V]]

  def add[V](name: String, value: V)(implicit at: AccumulatorType[V]): Accumulator[T, U] = {
    aggregator(name, at.sumFn()).addValue(value)
    this
  }

  def max[V](name: String, value: V)(implicit at: AccumulatorType[V]): Accumulator[T, U] = {
    aggregator(name, at.maxFn()).addValue(value)
    this
  }

  def min[V](name: String, value: V)(implicit at: AccumulatorType[V]): Accumulator[T, U] = {
    aggregator(name, at.minFn()).addValue(value)
    this
  }

}

private[dataflow] object FunctionsWithAccumulator {

  private abstract class DoFnWithAccumulator[T, U] extends DoFn[T, U] {
    var acc: Accumulator[T, U] = null
    override def startBundle(c: DoFn[T, U]#Context): Unit = acc = new Accumulator[T, U]()
  }

  def filterFn[T](f: (T, Accumulator[T, T]) => Boolean): DoFn[T, T] = new DoFnWithAccumulator[T, T] {
    val g = f  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit =
      if (g(c.element(), acc.withContext(c))) c.output(c.element())
  }

  def flatMapFn[T, U](f: (T, Accumulator[T, U]) => TraversableOnce[U]): DoFn[T, U] = new DoFnWithAccumulator[T, U] {
    val g = f  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit =
      g(c.element(), acc.withContext(c)).foreach(c.output)
  }

  def mapFn[T, U](f: (T, Accumulator[T, U]) => U): DoFn[T, U] = new DoFnWithAccumulator[T, U] {
    val g = f  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element(), acc.withContext(c)))
  }

}
