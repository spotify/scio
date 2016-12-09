package com.spotify.scio.named

import com.google.cloud.dataflow.sdk.transforms.{Combine, PTransform}
import com.google.cloud.dataflow.sdk.values.{PCollection, POutput}
import com.spotify.scio.util.CallSites
import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

trait LazySCollection[T] extends SCollection[T] {
  override val context: NamedScioContext
  val nameFn: String => PCollection[T]

  override lazy val internal = nameFn(CallSites.getCurrent) // TODO: Maybe not accurate

  override protected def pApply[U: ClassTag]
  (transform: PTransform[_ >: PCollection[T], PCollection[U]], name: String = CallSites.getCurrent)
  : SCollection[U] = {
    val t = if (classOf[Combine.Globally[T, U]] isAssignableFrom transform.getClass) {
      // In case PCollection is windowed
      transform.asInstanceOf[Combine.Globally[T, U]].withoutDefaults()
    } else {
      transform
    }
    context.wrap(this.applyInternal(t, _))
  }

  def withName(name: String): SCollection[T] = context.wrap(nameFn(name))
}

class LazySCollectionImpl[T: ClassTag](val nameFn: String => PCollection[T],
                                       val context: NamedScioContext)
  extends LazySCollection[T] {
  protected val ct = implicitly[ClassTag[T]]
}
