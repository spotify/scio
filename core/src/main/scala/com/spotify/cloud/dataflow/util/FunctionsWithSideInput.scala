package com.spotify.cloud.dataflow.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.spotify.cloud.dataflow.values.SideInputContext

private[dataflow] object FunctionsWithSideInput {

  def filterFn[T](f: (T, SideInputContext[T]) => Boolean): DoFn[T, T] = new DoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      // Workaround for type inference limit
      val ctx = new SideInputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      if (g(c.element(), ctx)) c.output(c.element())
    }
  }

  def flatMapFn[T, U](f: (T, SideInputContext[T]) => TraversableOnce[U]): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      // Workaround for type inference limit
      val ctx = new SideInputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      g(c.element(), ctx).foreach(c.output)
    }
  }

  def mapFn[T, U](f: (T, SideInputContext[T]) => U): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      // Workaround for type inference limit
      val ctx = new SideInputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      c.output(g(c.element(), ctx))
    }
  }

}
