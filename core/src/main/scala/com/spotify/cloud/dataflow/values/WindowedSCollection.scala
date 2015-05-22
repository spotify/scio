package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow
import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithWindowedValue
import org.joda.time.Instant

import scala.reflect.ClassTag

case class WindowedValue[T](value: T, timestamp: Instant, windows: Iterable[BoundedWindow])

class WindowedSCollection[T] private[values] (val internal: PCollection[T])
                                             (implicit private[values] val context: DataflowContext,
                                              protected val ct: ClassTag[T])
  extends PCollectionWrapper[T] {

  def filter(f: WindowedValue[T] => Boolean): WindowedSCollection[T] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.filterFn(f)).internal)

  def flatMap[U: ClassTag](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]]): WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.flatMapFn(f)).internal)

  def keyBy[K: ClassTag](f: WindowedValue[T] => K): WindowedSCollection[(K, T)] =
    this.map(wv => wv.copy(value = (f(wv), wv.value)))

  def map[U: ClassTag](f: WindowedValue[T] => WindowedValue[U]): WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.mapFn(f)).internal)

  def toSCollection: SCollection[T] = SCollection(internal)

}
