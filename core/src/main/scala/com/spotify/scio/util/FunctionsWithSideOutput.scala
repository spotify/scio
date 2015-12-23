package com.spotify.scio.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.spotify.scio.values.SideOutputContext

private[scio] object FunctionsWithSideOutput {

  trait SideOutputFn[T, U] extends DoFn[T, U] {
    private var ctx: SideOutputContext[T] = null
    def sideOutputContext(c: DoFn[T, U]#ProcessContext): SideOutputContext[T] = {
      if (ctx == null) {
        // Workaround for type inference limit
        ctx = new SideOutputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      }
      ctx
    }
  }

  def mapFn[T, U](f: (T, SideOutputContext[T]) => U): DoFn[T, U] = new SideOutputFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      c.output(g(c.element(), sideOutputContext(c)))
    }
  }

  def flatMapFn[T, U](f: (T, SideOutputContext[T]) => TraversableOnce[U]): DoFn[T, U] = new SideOutputFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      g(c.element(), sideOutputContext(c)).foreach(c.output)
    }
  }

}
