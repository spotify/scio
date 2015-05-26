package com.spotify.cloud.dataflow.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.spotify.cloud.dataflow.values.SideOutputContext

private[dataflow] object FunctionsWithSideOutput {

  def mapFn[T, U](f: (T, SideOutputContext[T]) => U): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      // Workaround for type inference limit
      val ctx = new SideOutputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      c.output(g(c.element(), ctx))
    }
  }

  def flatMapFn[T, U](f: (T, SideOutputContext[T]) => TraversableOnce[U]): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      // Workaround for type inference limit
      val ctx = new SideOutputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      g(c.element(), ctx).foreach(c.output)
    }
  }

}
