package com.spotify.scio.values
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder

/**
 * A read only container for collections which can be used to check for presence of an element.
 */
trait ApproxFilter[-T] extends (T => Boolean) {

  override def apply(t: T): Boolean = mayBeContains(t)

  def mayBeContains(t: T): Boolean

  def serialize: Array[Byte]
}

/**
 * A Builder is used to create [[ApproxFilter]]s from a given [[SCollection]]
 */
@experimental
trait ApproxFilterBuilder[T, To[B >: T] <: ApproxFilter[B]] {

  /** Build from an Iterable */
  def build(it: Iterable[T]): To[T]

  /**
   * Build a `SCollection[To[T]]` from an SCollection[T]
   *
   * By default groups all elements and builds the [[To]]
   */
  def build(sc: SCollection[T])(implicit coder: Coder[T],
                                approxFilterCoder: Coder[To[T]]): SCollection[To[T]] =
    sc.groupBy(_ => ())
      .values
      .map(build)

  def fromBytes(serializedBytes: Array[Byte]): To[T]
}
