package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.transforms.{Combine, ParDo, DoFn, PTransform}
import com.google.cloud.dataflow.sdk.values.{KV, POutput, PCollection}
import com.spotify.cloud.dataflow.{Implicits, DataflowContext}
import com.spotify.cloud.dataflow.util.CallSites

import scala.reflect.ClassTag

private[values] trait PCollectionWrapper[T] extends Implicits {

  val internal: PCollection[T]

  implicit val context: DataflowContext

  protected implicit val ct: ClassTag[T]

  def applyInternal[Output <: POutput](transform: PTransform[_ >: PCollection[T], Output]): Output =
    internal.apply(transform.withName(CallSites.getCurrent))

  protected def apply[U: ClassTag](transform: PTransform[_ >: PCollection[T], PCollection[U]]): SCollection[U] = {
    val t = if (classOf[Combine.Globally[T, U]] isAssignableFrom transform.getClass) {
      // in case PCollection is windowed
      transform.asInstanceOf[Combine.Globally[T, U]].withoutDefaults()
    } else {
      transform
    }
    SCollection(this.applyInternal(t))
  }

  protected def parDo[U: ClassTag](fn: DoFn[T, U]): SCollection[U] =
    this.apply(ParDo.of(fn)).setCoder(this.getCoder[U])

  private[values] def getCoder[U: ClassTag]: Coder[U] = internal.getPipeline.getCoderRegistry.getScalaCoder[U]

  private[values] def getKvCoder[K: ClassTag, V: ClassTag]: Coder[KV[K, V]] =
    internal.getPipeline.getCoderRegistry.getScalaKvCoder[K, V]

}
