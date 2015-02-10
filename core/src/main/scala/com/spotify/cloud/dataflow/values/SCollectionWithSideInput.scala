package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.{DoFn, ParDo}
import com.google.cloud.dataflow.sdk.values.{PCollection, PCollectionView}
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithSideInput

import scala.reflect.ClassTag

sealed trait SCollectionWithSideInput[T, S] {

  def filter(f: (T, S) => Boolean): SCollectionWithSideInput[T, S]

  def flatMap[U: ClassTag](f: (T, S) => TraversableOnce[U]): SCollectionWithSideInput[U, S]

  def keyBy[K: ClassTag](f: (T, S) => K): SCollectionWithSideInput[(K, T), S]

  def map[U: ClassTag](f: (T, S) => U): SCollectionWithSideInput[U, S]

  def toSCollection: SCollection[T]

}

private class SCollectionWithSideInputImpl[T, S, R](val internal: PCollection[T],
                                                        view: PCollectionView[R],
                                                        sideFn: R => S)
                                                       (implicit val context: DataflowContext, val ct: ClassTag[T])
  extends SCollectionWithSideInput[T, S] with PCollectionWrapper[T] {

  override protected def parDo[U: ClassManifest](fn: DoFn[T, U]): SCollection[U] =
    this.apply(ParDo.withSideInputs(view).of(fn)).setCoder(this.getCoder[U])

  def filter(f: (T, S) => Boolean): SCollectionWithSideInput[T, S] = {
    val o = this.parDo(FunctionsWithSideInput.filterFn(view, sideFn, f)).internal
    new SCollectionWithSideInputImpl[T, S, R](o, view, sideFn)
  }

  def flatMap[U: ClassTag](f: (T, S) => TraversableOnce[U]): SCollectionWithSideInput[U, S] = {
    val o = this.parDo(FunctionsWithSideInput.flatMapFn(view, sideFn, f)).internal
    new SCollectionWithSideInputImpl[U, S, R](o, view, sideFn)
  }

  def keyBy[K: ClassTag](f: (T, S) => K): SCollectionWithSideInput[(K, T), S] = this.map((v, c) => (f(v, c), v))

  def map[U: ClassTag](f: (T, S) => U): SCollectionWithSideInput[U, S] = {
    val o = this.parDo(FunctionsWithSideInput.mapFn(view, sideFn, f)).internal
    new SCollectionWithSideInputImpl[U, S, R](o, view, sideFn)
  }

  def toSCollection: SCollection[T] = SCollection(internal)

}
