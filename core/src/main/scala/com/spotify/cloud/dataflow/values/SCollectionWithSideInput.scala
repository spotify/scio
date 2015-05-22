package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithSideInput

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class SCollectionWithSideInput[T] private[values] (val internal: PCollection[T],
                                                   sides: Iterable[SideInput[_]])
                                                  (implicit private[values] val context: DataflowContext,
                                                   protected val ct: ClassTag[T])
  extends PCollectionWrapper[T] {

  private val parDo = ParDo.withSideInputs(sides.map(_.view).asJava)

  def filter(f: (T, SideInputContext[T]) => Boolean): SCollectionWithSideInput[T] = {
    val o = this.apply(parDo.of(FunctionsWithSideInput.filterFn(f))).internal.setCoder(this.getCoder[T])
    new SCollectionWithSideInput[T](o, sides)
  }

  def flatMap[U: ClassTag](f: (T, SideInputContext[T]) => TraversableOnce[U]): SCollectionWithSideInput[U] = {
    val o = this.apply(parDo.of(FunctionsWithSideInput.flatMapFn(f))).internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, sides)
  }

  def keyBy[K: ClassTag](f: (T, SideInputContext[T]) => K): SCollectionWithSideInput[(K, T)] =
    this.map((x, s) => (f(x, s), x))

  def map[U: ClassTag](f: (T, SideInputContext[T]) => U): SCollectionWithSideInput[U] = {
    val o = this.apply(parDo.of(FunctionsWithSideInput.mapFn(f))).internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, sides)
  }

  def toSCollection: SCollection[T] = SCollection(internal)

}
