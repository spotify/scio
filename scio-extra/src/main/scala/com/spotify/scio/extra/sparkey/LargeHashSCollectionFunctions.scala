package com.spotify.scio.extra.sparkey

import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.sparkey.instances.SparkeySet
import com.spotify.scio.values.{SCollection, SideInput}

/** Extra functions available on SCollections for Sparkey hash-based filtering. */
class LargeHashSCollectionFunctions[T](val self: SCollection[T]) {

  /**
   * Return a new SCollection containing only elements that also exist in the `LargeSetSideInput`.
   *
   * @group transform
   */
  def hashFilter(sideInput: SideInput[SparkeySet[T]])(implicit coder: Coder[T]): SCollection[T] =
    self.map((_, ())).hashIntersectByKey(sideInput).keys
}
