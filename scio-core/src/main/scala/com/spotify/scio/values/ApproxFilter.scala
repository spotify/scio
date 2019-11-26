package com.spotify.scio.values
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder

/**
 * A read only container for collections which can be used to check approximate
 * membership queries.
 *
 * Constructors for [[ApproxFilter]] are defined using [[ApproxFilterBuilder]]
 */
trait ApproxFilter[-T] extends (T => Boolean) {

  override def apply(t: T): Boolean = mayBeContains(t)

  /**
   * Check if the filter may contain a given element.
   */
  def mayBeContains(t: T): Boolean

  /**
   * Serialize the Filter to an Array[Byte]
   */
  def serialize: Array[Byte]
}

/**
 * An ApproxFilterBuilder[T, To] is used to create [[ApproxFilter]] of type [[To]]
 * from various source Collections which contain elements of type [T]
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
                                filterCoder: Coder[To[T]]): SCollection[To[T]] =
    sc.groupBy(_ => ())
      .values
      .map(build)

  /**
   * Read from serialized bytes to this filter.
   *
   * The serialization is done using `ApproxFilter[T]#serialize`
   */
  def fromBytes(serializedBytes: Array[Byte]): To[T]
}
