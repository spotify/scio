package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithAccumulator

import scala.reflect.ClassTag

class SCollectionWithAccumulator[T] private[values] (val internal: PCollection[T])
                                                    (implicit private[values] val context: DataflowContext,
                                                     protected val ct: ClassTag[T])
  extends PCollectionWrapper[T] {

  def filter(f: (T, Accumulator[T, T]) => Boolean): SCollectionWithAccumulator[T] =
    new SCollectionWithAccumulator(this.parDo(FunctionsWithAccumulator.filterFn(f)).internal)

  def flatMap[U: ClassTag](f: (T, Accumulator[T, U]) => TraversableOnce[U]): SCollectionWithAccumulator[U] =
    new SCollectionWithAccumulator(this.parDo(FunctionsWithAccumulator.flatMapFn(f)).internal)

  def keyBy[K: ClassTag](f: (T, Accumulator[T, (K, T)]) => K): SCollectionWithAccumulator[(K, T)] =
    this.map((v, a) => (f(v, a), v))

  def map[U: ClassTag](f: (T, Accumulator[T, U]) => U): SCollectionWithAccumulator[U] =
    new SCollectionWithAccumulator(this.parDo(FunctionsWithAccumulator.mapFn(f)).internal)

  def toSCollection: SCollection[T] = SCollection(internal)

}
