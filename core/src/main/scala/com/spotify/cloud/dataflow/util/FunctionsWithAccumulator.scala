package com.spotify.cloud.dataflow.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.spotify.cloud.dataflow.values.Accumulator

private[dataflow] object FunctionsWithAccumulator {

  private abstract class DoFnWithAccumulator[T, U] extends DoFn[T, U] {
    var acc: Accumulator[T, U] = null
    override def startBundle(c: DoFn[T, U]#Context): Unit = acc = new Accumulator[T, U]()
  }

  def filterFn[T](f: (T, Accumulator[T, T]) => Boolean): DoFn[T, T] = new DoFnWithAccumulator[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit =
      if (g(c.element(), acc.withContext(c))) c.output(c.element())
  }

  def flatMapFn[T, U](f: (T, Accumulator[T, U]) => TraversableOnce[U]): DoFn[T, U] = new DoFnWithAccumulator[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit =
      g(c.element(), acc.withContext(c)).foreach(c.output)
  }

  def mapFn[T, U](f: (T, Accumulator[T, U]) => U): DoFn[T, U] = new DoFnWithAccumulator[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element(), acc.withContext(c)))
  }

}
