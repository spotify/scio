package com.spotify.cloud.dataflow.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.PCollectionView

private[dataflow] object FunctionsWithSideInput {

  private abstract class DoFnWithSideInput[T, U, S, W, C](private val view: PCollectionView[S],
                                                          private val sideFn: S => C) extends DoFn[T, U] {

    protected var _side: C = null.asInstanceOf[C]

    protected def side[U](c: DoFn[T, U]#ProcessContext): C = {
      if (_side == null) {
        _side = sideFn(c.sideInput(view))
      }
      _side
    }

  }

  def filterFn[T, S, W, C](view: PCollectionView[S], sideFn: S => C, f: (T, C) => Boolean): DoFn[T, T] =
    new DoFnWithSideInput[T, T, S, W, C](view, sideFn) {
      val g = f  // defeat closure
      override def processElement(c: DoFn[T, T]#ProcessContext): Unit =
        if (g(c.element(), side(c))) c.output(c.element())
    }

  def flatMapFn[T, U, S, W, C](view: PCollectionView[S],
                               sideFn: S => C,
                               f: (T, C) => TraversableOnce[U]): DoFn[T, U] =
    new DoFnWithSideInput[T, U, S, W, C](view, sideFn) {
      val g = f  // defeat closure
      override def processElement(c: DoFn[T, U]#ProcessContext): Unit = g(c.element(), side(c)).foreach(c.output)
    }

  def mapFn[T, U, S, W, C](view: PCollectionView[S], sideFn: S => C, f: (T, C) => U): DoFn[T, U] =
    new DoFnWithSideInput[T, U, S, W, C](view, sideFn) {
      val g = f  // defeat closure
      override def processElement(c: DoFn[T, U]#ProcessContext): Unit = c.output(g(c.element(), side(c)))
    }

}
