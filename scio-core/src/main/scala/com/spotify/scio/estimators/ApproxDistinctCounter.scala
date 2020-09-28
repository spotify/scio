package com.spotify.scio.estimators

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{PairSCollectionFunctions, SCollection}
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{KV, PCollection}

/**
 * Approximate distinct element counter for type `T`, e.g. HyperLogLog or HyperLogLog++. This has two APIs one
 * estimate total distinct count for a given PCollection
 * @tparam T
 */
trait ApproxDistinctCounter[T] {

  def estimateDistinctCount(in: PCollection[T]): PCollection[java.lang.Long]

  def estimateDistinctPerKey[K](in: PCollection[KV[K, T]]): PCollection[KV[K, java.lang.Long]]
}

object ApproxDistinctCounter {

  implicit class EstimateDistinctGlobally[T](@transient private val self: SCollection[T])
      extends AnyVal {

    def estimateDistinctCount(estimator: ApproxDistinctCounter[T]): SCollection[Long] = {
      self
        .applyTransform(
          new PTransform[PCollection[T], PCollection[java.lang.Long]]("Estimate distinct count") {
            override def expand(input: PCollection[T]): PCollection[java.lang.Long] =
              estimator.estimateDistinctCount(input)
          }
        )
        .map(Long2long)
    }
  }

  implicit class EstimateDistinctPerKey[K, V](
    @transient private val pairSCol: PairSCollectionFunctions[K, V]
  ) extends AnyVal {

    import com.spotify.scio.values.SCollection._

    def estimateDistinctPerKey(
      estimator: ApproxDistinctCounter[V]
    )(implicit kCoder: Coder[K], vCoder: Coder[V]): PairSCollectionFunctions[K, Long] = {

      pairSCol.self
        .transform(
          _.withName("Tuple to KV").toKV
            .applyTransform(
              new PTransform[PCollection[KV[K, V]], PCollection[KV[K, java.lang.Long]]](
                "Estimate distinct count per key"
              ) {
                override def expand(
                  input: PCollection[KV[K, V]]
                ): PCollection[KV[K, java.lang.Long]] =
                  estimator.estimateDistinctPerKey(input)
              }
            )
            .map(kv => (kv.getKey, Long2long(kv.getValue)))
        )
    }
  }
}
//
//abstract class HyperLogLogPlusPlus[T]() extends DistinctCountEstimator[T] {
//
//  /**
//   * Precision:
//   * Controls the accuracy of the estimation. The precision value will have an impact on the number of buckets used
//   * to store information about the distinct elements.
//   * In general one can expect a relative error of about 1.1 / sqrt(2&#94;p).
//   * The value should be of at least 4 to guarantee a minimal accuracy.
//   */
//  val p: Int
//
//  /**
//   * Sparse Precision:
//   * Used to create a sparse representation in order to optimize memory and improve accuracy at small cardinalities.
//   * The value of sp should be greater than p(precision), but lower than 32.
//   */
//  val sp: Int
//
//}
