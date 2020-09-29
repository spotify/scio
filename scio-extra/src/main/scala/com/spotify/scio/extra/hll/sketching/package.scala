package com.spotify.scio.extra.hll

import com.spotify.scio.coders.Coder
import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.TupleFunctions._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sketching.ApproximateDistinct

package object sketching {

  /**
   * [[com.spotify.scio.estimators.ApproxDistinctCounter]] implementation for
   * [[org.apache.beam.sdk.extensions.sketching.ApproximateDistinct]], ApproximateDistinct estimate the distinct count
   * using HyperLogLog++.
   *
   * The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a data stream.
   * HLL++ is based on HyperLogLog; HLL++ more accurately estimates the number of distinct values in very large and
   * small data streams.
   *
   * @param p Precision,
   *          Controls the accuracy of the estimation. The precision value will have an impact on the number of
   *          buckets used  to store information about the distinct elements. In general one can expect a relative
   *          error of about 1.1 / sqrt(2&#94;p). The value should be of at least 4 to guarantee a minimal accuracy.
   *
   * @param sp Sparse Precision,
   *           Uses to create a sparse representation in order to optimize memory and improve accuracy at small
   *           cardinalities. The value of sp should be greater than p(precision), but lower than 32.
   */
  case class SketchingHyperLogLogPlusPlus[T](p: Int, sp: Int) extends ApproxDistinctCounter[T] {
    override def estimateDistinctCount(in: SCollection[T]): SCollection[Long] =
      in.applyTransform(
        "Approximate distinct count",
        ApproximateDistinct
          .globally[T]()
          .withPrecision(p)
          .withSparsePrecision(sp)
      ).asInstanceOf[SCollection[Long]]

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, T)]
    )(implicit koder: Coder[K], voder: Coder[T]): SCollection[(K, Long)] = {
      val inKv = in.toKV
      inKv
        .applyTransform(
          "Approximate distinct count per key",
          ApproximateDistinct
            .perKey[K, T]()
            .withPrecision(p)
            .withSparsePrecision(sp)
        )
        .map(klToTuple)
    }
  }
}
