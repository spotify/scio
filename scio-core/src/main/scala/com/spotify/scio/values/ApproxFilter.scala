package com.spotify.scio.values
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}

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
  def toBytes: Array[Byte] = {
    val ba = new ByteArrayOutputStream()
    writeTo(ba)
    ba.toByteArray
  }

  /**
   * Serialize the filter to the given [[OutputStream]]
   */
  def writeTo(out: OutputStream): Unit
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
    sc.transform("Create Approx Filter")(
      _.groupBy(_ => ())
      .values
      .map(build)
    )

  /**
   * Read from serialized bytes to this filter.
   *
   * The serialization is done using `ApproxFilter[T]#toBytes`
   */
  def fromBytes(serializedBytes: Array[Byte]): To[T] = {
    readFrom(new ByteArrayInputStream(serializedBytes))
  }

  def readFrom(in: InputStream): To[T]
}
