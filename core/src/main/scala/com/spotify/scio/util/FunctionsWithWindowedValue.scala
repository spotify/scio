package com.spotify.scio.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess
import com.spotify.scio.values.WindowedValue
import org.joda.time.Instant

import scala.collection.JavaConverters._

private[scio] object FunctionsWithWindowedValue {

  abstract class WindowDoFn[T, U] extends DoFn[T, U] with RequiresWindowAccess

  def filterFn[T, U](f: WindowedValue[T] => Boolean): DoFn[T, T] = new WindowDoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      val wv = WindowedValue(c.element(), c.timestamp(), c.window())
      if (g(wv)) c.output(c.element())
    }
  }

  def flatMapFn[T, U](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]]): DoFn[T, U] = new WindowDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val wv = WindowedValue(c.element(), c.timestamp(), c.window())
      g(wv).foreach(v => c.outputWithTimestamp(v.value, v.timestamp))
    }
  }

  def mapFn[T, U](f: WindowedValue[T] => WindowedValue[U]): DoFn[T, U] = new WindowDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val wv = g(WindowedValue(c.element(), c.timestamp(), c.window()))
      c.outputWithTimestamp(wv.value, wv.timestamp)
    }
  }

  def timestampFn[T](f: T => Instant): DoFn[T, T] = new WindowDoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      val t = c.element()
      c.outputWithTimestamp(t, g(t))
    }
  }

}
