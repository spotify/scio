package com.spotify.scio.values
import java.io.Serializable

import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder

/**
 * An `ApproxFilterBuilder[T, To]` is used to create [[ApproxFilter]] of type [[To]]
 * from various source collections which contain elements of type [T]
 *
 * These are implemented for each ApproxFilter and are used for creating the filters.
 * Different instances of an [[ApproxFilterBuilder]] are available via constructors
 * in the [[ApproxFilter]]'s companion object. The constructor can require multiple
 * runtime parameters and configurations like expected insertions / false positive
 * probabilities to define a builder. Hence a Builder is not available as an implicit.
 * However the constructors might summon other implicit type class instances before
 * providing a Builder.
 */
@experimental
trait ApproxFilterBuilder[T, To[B >: T] <: ApproxFilter[B]] extends Serializable {
  /**
   * The name of this builder.
   * This name shows up nicely as a transform name for the pipeline.
   */
  def name: String = this.getClass.getSimpleName

  /** Build from an Iterable */
  def build(it: Iterable[T]): To[T]

  /**
   * Build a `SCollection[To[T]]` from an SCollection[T]
   *
   * By default groups all elements and builds the [[To]]
   */
  def build(
             sc: SCollection[T]
           )(implicit coder: Coder[T], filterCoder: Coder[To[T]]): SCollection[To[T]] =
    sc.transform(name)(
      _.distinct
        .groupBy(_ => ())
        .values
        .map(build)
    )
}
