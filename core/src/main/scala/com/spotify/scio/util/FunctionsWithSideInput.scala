package com.spotify.scio.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.spotify.scio.values.SideInputContext

private[scio] object FunctionsWithSideInput {

  trait SideInputDoFn[T, U] extends DoFn[T, U] {
    private var ctx: SideInputContext[T] = null
    def sideInputContext(c: DoFn[T, U]#ProcessContext): SideInputContext[T] = {
      if (ctx == null) {
        // Workaround for type inference limit
        ctx = new SideInputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      }
      ctx
    }
  }

  def filterFn[T](f: (T, SideInputContext[T]) => Boolean): DoFn[T, T] = new SideInputDoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit =
      if (g(c.element(), sideInputContext(c))) {
        c.output(c.element())
      }
  }

  def flatMapFn[T, U](f: (T, SideInputContext[T]) => TraversableOnce[U]): DoFn[T, U] = new SideInputDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit =
      g(c.element(), sideInputContext(c)).foreach(c.output)
  }

  def mapFn[T, U](f: (T, SideInputContext[T]) => U): DoFn[T, U] = new SideInputDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element(), sideInputContext(c)))
  }

}
