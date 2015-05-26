package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithAccumulator

import scala.reflect.ClassTag

/** An enhanced SCollection that provides access to an [[Accumulator]] for some transforms. */
class SCollectionWithAccumulator[T] private[values] (val internal: PCollection[T])
                                                    (implicit private[values] val context: DataflowContext,
                                                     protected val ct: ClassTag[T])
  extends PCollectionWrapper[T] {

  /** [[SCollection.filter]] with an additional Accumulator argument. */
  def filter(f: (T, Accumulator[T, T]) => Boolean): SCollectionWithAccumulator[T] =
    new SCollectionWithAccumulator(this.parDo(FunctionsWithAccumulator.filterFn(f)).internal)

  /** [[SCollection.flatMap]] with an additional Accumulator argument. */
  def flatMap[U: ClassTag](f: (T, Accumulator[T, U]) => TraversableOnce[U]): SCollectionWithAccumulator[U] =
    new SCollectionWithAccumulator(this.parDo(FunctionsWithAccumulator.flatMapFn(f)).internal)

  /** [[SCollection.keyBy]] with an additional Accumulator argument. */
  def keyBy[K: ClassTag](f: (T, Accumulator[T, (K, T)]) => K): SCollectionWithAccumulator[(K, T)] =
    this.map((v, a) => (f(v, a), v))

  /** [[SCollection.map]] with an additional Accumulator argument. */
  def map[U: ClassTag](f: (T, Accumulator[T, U]) => U): SCollectionWithAccumulator[U] =
    new SCollectionWithAccumulator(this.parDo(FunctionsWithAccumulator.mapFn(f)).internal)

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = SCollection(internal)

}
