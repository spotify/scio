package com.spotify.scio.values

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
trait ApproxFilterBuilder[T, To[B >: T] <: ApproxFilter[B]] {

  /**
   * Build a `SCollection[To[T]]` from an SCollection[T]
   */
  def build(sc: SCollection[T]): SCollection[To[T]]

  def fromBytes(serializedBytes: Array[Byte]): To[T]
}
