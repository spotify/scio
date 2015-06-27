package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithAccumulator

import scala.reflect.ClassTag

/**
 * An enhanced SCollection that provides access to one or more [[Accumulator]]s for some
 * transforms. [[Accumulator]]s are accessed via the additional [[AccumulatorContext]] argument.
 */
class SCollectionWithAccumulator[T: ClassTag] private[values] (val internal: PCollection[T],
                                                               private[values] val context: DataflowContext,
                                                               acc: Seq[Accumulator[_]])
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  /** [[SCollection.filter]] with an additional AccumulatorContext argument. */
  def filter(f: (T, AccumulatorContext) => Boolean): SCollectionWithAccumulator[T] = {
    val o = this.parDo(FunctionsWithAccumulator.filterFn(f, acc))
    new SCollectionWithAccumulator[T](o.internal, context, acc)
  }

  /** [[SCollection.flatMap]] with an additional AccumulatorContext argument. */
  def flatMap[U: ClassTag](f: (T, AccumulatorContext) => TraversableOnce[U]): SCollectionWithAccumulator[U] = {
    val o = this.parDo(FunctionsWithAccumulator.flatMapFn(f, acc))
    new SCollectionWithAccumulator[U](o.internal, context, acc)
  }

  /** [[SCollection.map]] with an additional AccumulatorContext argument. */
  def map[U: ClassTag](f: (T, AccumulatorContext) => U): SCollectionWithAccumulator[U] = {
    val o = this.parDo(FunctionsWithAccumulator.mapFn(f, acc))
    new SCollectionWithAccumulator[U](o.internal, context, acc)
  }

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = context.wrap(internal)

}
