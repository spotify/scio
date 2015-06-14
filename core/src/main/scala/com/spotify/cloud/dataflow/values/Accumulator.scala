package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.Max.{MaxDoubleFn, MaxIntegerFn, MaxLongFn}
import com.google.cloud.dataflow.sdk.transforms.Min.{MinDoubleFn, MinIntegerFn, MinLongFn}
import com.google.cloud.dataflow.sdk.transforms.Sum.{SumDoubleFn, SumIntegerFn, SumLongFn}
import com.google.cloud.dataflow.sdk.transforms.{Aggregator, Combine, DoFn}

import scala.collection.mutable.{Map => MMap}

/** Type class for `T` that can be used in an [[Accumulator]]. */
sealed trait AccumulatorType[T] {
  type CF = CombineFn[T, Array[T], T]
  type BCF = Combine.BinaryCombineFn[T]

  protected def sumFnImpl(): CombineFn[_, _, _]
  protected def minFnImpl(): Combine.BinaryCombineFn[_]
  protected def maxFnImpl(): Combine.BinaryCombineFn[_]

  /** CombineFn for computing sum of the underlying values. */
  def sumFn(): CF = sumFnImpl().asInstanceOf[CF]

  /** BinaryCombineFn for computing maximum of the underlying values. */
  def minFn(): BCF = minFnImpl().asInstanceOf[BCF]

  /** BinaryCombineFn for computing minimum of the underlying values. */
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

/**
 * Accumulator of values that can be updated via `add`, `max` or `min` operation, similar to
 * Hadoop counters. Each accumulation must have a unique name.
 */
class Accumulator[T, U] private[dataflow]() {

  private val m: MMap[String, Aggregator[_, _]] = MMap.empty
  private var c: DoFn[T, U]#ProcessContext = null

  private[dataflow] def withContext(c: DoFn[T, U]#ProcessContext): Accumulator[T, U] = {
    this.c = c
    this
  }

  private def aggregator[V, VA](name: String, f: CombineFn[V, VA, V]): Aggregator[V, V] = {
    // m.getOrElseUpdate(name, c.createAggregator(name, f)).asInstanceOf[Aggregator[V]]
    new Aggregator[V, V] {
      override def getCombineFn: CombineFn[V, V, V] = null
      override def getName: String = name
      override def addValue(value: V): Unit = {}
    }
  }

  /** Add value to the named accumulation. */
  def add[V](name: String, value: V)(implicit at: AccumulatorType[V]): Accumulator[T, U] = {
    aggregator(name, at.sumFn()).addValue(value)
    this
  }

  /** Update maximum value of the named accumulation. */
  def max[V](name: String, value: V)(implicit at: AccumulatorType[V]): Accumulator[T, U] = {
    aggregator(name, at.maxFn()).addValue(value)
    this
  }

  /** Update minimum value of the named accumulation. */
  def min[V](name: String, value: V)(implicit at: AccumulatorType[V]): Accumulator[T, U] = {
    aggregator(name, at.minFn()).addValue(value)
    this
  }

}
