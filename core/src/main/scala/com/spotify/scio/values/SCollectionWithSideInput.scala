package com.spotify.scio.values

import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.util.FunctionsWithSideInput

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * An enhanced SCollection that provides access to one or more [[SideInput]]s for some transforms.
 * [[SideInput]]s are accessed via the additional [[SideInputContext]] argument.
 */
class SCollectionWithSideInput[T: ClassTag] private[values] (val internal: PCollection[T],
                                                             private[scio] val context: ScioContext,
                                                             sides: Iterable[SideInput[_]])
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  private val parDo = ParDo.withSideInputs(sides.map(_.view).asJava)

  /** [[SCollection.filter]] with an additional SideInputContext argument. */
  def filter(f: (T, SideInputContext[T]) => Boolean): SCollectionWithSideInput[T] = {
    val o = this.apply(parDo.of(FunctionsWithSideInput.filterFn(f))).internal.setCoder(this.getCoder[T])
    new SCollectionWithSideInput[T](o, context, sides)
  }

  /** [[SCollection.flatMap]] with an additional SideInputContext argument. */
  def flatMap[U: ClassTag](f: (T, SideInputContext[T]) => TraversableOnce[U]): SCollectionWithSideInput[U] = {
    val o = this.apply(parDo.of(FunctionsWithSideInput.flatMapFn(f))).internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, context, sides)
  }

  /** [[SCollection.keyBy]] with an additional SideInputContext argument. */
  def keyBy[K: ClassTag](f: (T, SideInputContext[T]) => K): SCollectionWithSideInput[(K, T)] =
    this.map((x, s) => (f(x, s), x))

  /** [[SCollection.map]] with an additional SideInputContext argument. */
  def map[U: ClassTag](f: (T, SideInputContext[T]) => U): SCollectionWithSideInput[U] = {
    val o = this.apply(parDo.of(FunctionsWithSideInput.mapFn(f))).internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, context, sides)
  }

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = context.wrap(internal)

}
