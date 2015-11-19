package com.spotify.scio.values

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.transforms.{Combine, DoFn, PTransform, ParDo}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection, POutput}
import com.spotify.scio.util.CallSites
import com.spotify.scio.{Implicits, ScioContext}

import scala.reflect.ClassTag

private[values] trait PCollectionWrapper[T] {

  import Implicits._

  /** The PCollection being wrapped internally. */
  val internal: PCollection[T]

  private[scio] val context: ScioContext

  implicit protected val ct: ClassTag[T]

  private[scio] def applyInternal[Output <: POutput](transform: PTransform[_ >: PCollection[T], Output]): Output =
    internal.apply(CallSites.getCurrent, transform)

  protected def apply[U: ClassTag](transform: PTransform[_ >: PCollection[T], PCollection[U]]): SCollection[U] = {
    val t = if (classOf[Combine.Globally[T, U]] isAssignableFrom transform.getClass) {
      // In case PCollection is windowed
      transform.asInstanceOf[Combine.Globally[T, U]].withoutDefaults()
    } else {
      transform
    }
    context.wrap(this.applyInternal(t))
  }

  private[scio] def parDo[U: ClassTag](fn: DoFn[T, U]): SCollection[U] =
    this.apply(ParDo.of(fn)).setCoder(this.getCoder[U])

  private[values] def getCoder[U: ClassTag]: Coder[U] = internal.getPipeline.getCoderRegistry.getScalaCoder[U]

  private[values] def getKvCoder[K: ClassTag, V: ClassTag]: Coder[KV[K, V]] =
    internal.getPipeline.getCoderRegistry.getScalaKvCoder[K, V]

}
