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

  protected def _sumFn(): CombineFn[_, _, _]
  protected def _minFn(): Combine.BinaryCombineFn[_]
  protected def _maxFn(): Combine.BinaryCombineFn[_]

  def sumFn() = _sumFn().asInstanceOf[CF]
  def minFn() = _minFn().asInstanceOf[BCF]
  def maxFn() = _maxFn().asInstanceOf[BCF]
}

private[dataflow] class IntAccumulatorType extends AccumulatorType[Int] {
  override def _sumFn() = new SumIntegerFn()
  override def _minFn() = new MinIntegerFn()
  override def _maxFn() = new MaxIntegerFn()
}

private[dataflow] class LongAccumulatorType extends AccumulatorType[Long] {
  override def _sumFn() = new SumLongFn()
  override def _minFn() = new MinLongFn()
  override def _maxFn() = new MaxLongFn()
}

private[dataflow] class DoubleAccumulatorType extends AccumulatorType[Double] {
  override def _sumFn() =  new SumDoubleFn()
  override def _minFn() = new MinDoubleFn()
  override def _maxFn() = new MaxDoubleFn()
}

class Accumulator[T, U] {

  private val m: MMap[String, Aggregator[_]] = MMap.empty
  private var c: DoFn[T, U]#ProcessContext = null

  def withContext(c: DoFn[T, U]#ProcessContext): Accumulator[T, U] = {
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