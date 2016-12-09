package com.spotify.scio.named

import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.values.SCollection
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}

import scala.reflect.ClassTag


object NamedContextAndArgs {
  /** Create [[NamedScioContext]] and [[Args]] for command line arguments. */
  def apply(args: Array[String]): (ScioContext, Args) = {
    val (sc, _args) = ContextAndArgs(args)
    (NamedScioContext(sc), _args)
  }
}

object NamedScioContext {
  def apply(sc: ScioContext) = new NamedScioContext(sc)
  def forTest(): ScioContext = NamedScioContext(ScioContext.forTest())
}

private[scio] class NamedScioContext(delegate: ScioContext)
  extends ScioContext(options = delegate.options, artifacts = delegate.artifacts)  {

  override def wrap[T: ClassTag](p: PCollection[T]): SCollection[T] =
    wrap(_ => p)

  def wrap[T: ClassTag](nameFn: String => PCollection[T]): SCollection[T] = {
    new LazySCollectionImpl[T](nameFn, this)
  }
}
