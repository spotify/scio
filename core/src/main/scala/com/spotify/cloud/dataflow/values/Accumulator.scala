package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.Max.{MaxDoubleFn, MaxIntegerFn, MaxLongFn}
import com.google.cloud.dataflow.sdk.transforms.Min.{MinDoubleFn, MinIntegerFn, MinLongFn}
import com.google.cloud.dataflow.sdk.transforms.Sum.{SumDoubleFn, SumIntegerFn, SumLongFn}
import com.google.cloud.dataflow.sdk.transforms.{Aggregator, Combine, DoFn}

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

class Accumulator[T, U] private[dataflow]() {

  private val m: MMap[String, Aggregator[_]] = MMap.empty
  private var c: DoFn[T, U]#ProcessContext = null

  private[dataflow] def withContext(c: DoFn[T, U]#ProcessContext): Accumulator[T, U] = {
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
