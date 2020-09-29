package com.spotify.scio.extra.hll

import com.spotify.scio.coders.Coder
import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.TupleFunctions._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.zetasketch.HllCount

package object zetasketch {

  /** @param p */
  case class ZetasketchHllCountInt(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[Int] {

    override def estimateDistinctCount(in: SCollection[Int]): SCollection[Long] = {
      in.map(int2Integer)
        .applyTransform(HllCount.Init.forIntegers().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .map(Long2long)
    }

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, Int)]
    )(implicit koder: Coder[K], voder: Coder[Int]): SCollection[(K, Long)] =
      in.mapValues(int2Integer)
        .toKV
        .applyTransform(HllCount.Init.forIntegers().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)
  }
}
