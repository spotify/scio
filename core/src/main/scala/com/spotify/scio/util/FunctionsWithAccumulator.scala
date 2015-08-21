package com.spotify.scio.util

import com.google.cloud.dataflow.sdk.transforms.{Aggregator, DoFn}
import com.spotify.scio.values.{Accumulator, AccumulatorContext}

import scala.reflect.ClassTag

abstract private class DoFnWithAccumulator[I, O](acc: Seq[Accumulator[_]]) extends DoFn[I, O] {
  private val m = {
    val b = Map.newBuilder[String, Aggregator[_, _]]
    acc.foreach(a => b += (a.name -> this.createAggregator(a.name, a.combineFn)))
    b.result()
  }

  protected def context: AccumulatorContext = new AccumulatorContext(m)
}

private[scio] object FunctionsWithAccumulator {

  def filterFn[T](f: (T, AccumulatorContext) => Boolean,
                  acc: Seq[Accumulator[_]]): DoFn[T, T] = new DoFnWithAccumulator[T, T](acc) {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      if (g(c.element(), this.context)) c.output(c.element())
    }
  }

  def flatMapFn[T, U: ClassTag](f: (T, AccumulatorContext) => TraversableOnce[U],
                                acc: Seq[Accumulator[_]]): DoFn[T, U] = new DoFnWithAccumulator[T, U](acc) {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = g(c.element(), this.context).foreach(c.output)
  }

  def mapFn[T, U: ClassTag](f: (T, AccumulatorContext) => U,
                            acc: Seq[Accumulator[_]]): DoFn[T, U] = new DoFnWithAccumulator[T, U](acc) {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element(), this.context))
  }

}
