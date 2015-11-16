package com.spotify.scio.values

import com.google.cloud.dataflow.sdk.transforms.windowing.{BoundedWindow, PaneInfo, Trigger}
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode
import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.util.FunctionsWithWindowedValue
import org.joda.time.{Duration, Instant}

import scala.reflect.ClassTag

case class WindowOptions[W <: BoundedWindow](allowedLateness: Duration = null,
                                             trigger: Trigger[W] = null,
                                             accumulationMode: AccumulationMode = null)

case class WindowedValue[T](value: T, timestamp: Instant, window: BoundedWindow, pane: PaneInfo) {
  def withValue[U](v: U): WindowedValue[U] = WindowedValue(v, this.timestamp, this.window, this.pane)
}

class WindowedSCollection[T: ClassTag] private[values] (val internal: PCollection[T],
                                                        private[scio] val context: ScioContext)
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  def filter(f: WindowedValue[T] => Boolean): WindowedSCollection[T] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.filterFn(f)).internal, context)

  def flatMap[U: ClassTag](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]]): WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.flatMapFn(f)).internal, context)

  def keyBy[K: ClassTag](f: WindowedValue[T] => K): WindowedSCollection[(K, T)] =
    this.map(wv => wv.copy(value = (f(wv), wv.value)))

  def map[U: ClassTag](f: WindowedValue[T] => WindowedValue[U]): WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.mapFn(f)).internal, context)

  def toSCollection: SCollection[T] = context.wrap(internal)

}
