package com.spotify.cloud.dataflow.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.spotify.cloud.dataflow.values.WindowedValue
import org.joda.time.Instant

import scala.collection.JavaConverters._

private[dataflow] object FunctionsWithWindowedValue {

  def filterFn[T, U](f: WindowedValue[T] => Boolean): DoFn[T, T] = new DoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      val wv = WindowedValue(c.element(), c.timestamp(), c.windowingInternals().windows().asScala)
      if (g(wv)) c.output(c.element())
    }
  }

  def flatMapFn[T, U](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]]): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val wv = WindowedValue(c.element(), c.timestamp(), c.windowingInternals().windows().asScala)
      g(wv).foreach(v => c.outputWithTimestamp(v.value, v.timestamp))
    }
  }

  def mapFn[T, U](f: WindowedValue[T] => WindowedValue[U]): DoFn[T, U] = new DoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val wv = g(WindowedValue(c.element(), c.timestamp(), c.windowingInternals().windows().asScala))
      c.outputWithTimestamp(wv.value, wv.timestamp)
    }
  }

  def timestampFn[T](f: T => Instant): DoFn[T, T] = new DoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      val t = c.element()
      c.outputWithTimestamp(t, g(t))
    }
  }

}
