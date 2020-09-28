package com.spotify.scio.extra.hll

import com.spotify.scio.estimators.ApproxDistinctCounter
import org.apache.beam.sdk.values.{KV, PCollection}

package object sketching {

  /**
   * @param p
   * Precision: p
   * Controls the accuracy of the estimation. The precision value will have an impact on the number of buckets used
   * to store information about the distinct elements.
   * In general one can expect a relative error of about 1.1 / sqrt(2&#94;p).
   * The value should be of at least 4 to guarantee a minimal accuracy.
   *
   * @param sp
   * Sparse Precision: sp
   * Used to create a sparse representation in order to optimize memory and improve accuracy at small cardinalities.
   * The value of sp should be greater than p(precision), but lower than 32.
   */
  case class SketchingHyperLogLogPlusPlus[T](p: Int, sp: Int) extends ApproxDistinctCounter[T] {
    import org.apache.beam.sdk.extensions.sketching.ApproximateDistinct
    override def estimateDistinctCount(in: PCollection[T]): PCollection[java.lang.Long] =
      ApproximateDistinct
        .globally[T]()
        .withPrecision(p)
        .withSparsePrecision(sp)
        .expand(in)

    override def estimateDistinctPerKey[K](
      in: PCollection[KV[K, T]]
    ): PCollection[KV[K, java.lang.Long]] =
      ApproximateDistinct
        .perKey[K, T]()
        .withPrecision(p)
        .withSparsePrecision(sp)
        .expand(in)
  }
}
