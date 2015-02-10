package com.spotify.cloud.dataflow.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.TupleTag

private[dataflow] object FunctionsWithSideOutput {

  def mapFn[T, U, S](f: T => (U, S), s: TupleTag[S]): DoFn[T, U] = new DoFn[T, U] {
    // defeat closure
    val g = f
    val t = s

    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val (o, s) = f(c.element())
      c.output(o)
      c.sideOutput(t, s)
    }
  }

  def flatMapFn[T, U, S](f: T => (TraversableOnce[U], TraversableOnce[S]),
                            s: TupleTag[S]): DoFn[T, U] = new DoFn[T, U] {
    // defeat closure
    val g = f
    val t = s

    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val (o, s) = f(c.element())
      o.foreach(c.output)
      s.foreach(c.sideOutput(t, _))
    }
  }

}
