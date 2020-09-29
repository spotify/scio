package com.spotify.scio.extra.hll

import com.spotify.scio.coders.Coder
import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.TupleFunctions._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.zetasketch.HllCount

package object zetasketch {

  case class ZetasketchHllIntCounter(p: Int = HllCount.DEFAULT_PRECISION)
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

  case class ZetasketchHllLongCounter(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[Long] {

    override def estimateDistinctCount(in: SCollection[Long]): SCollection[Long] =
      in.map(long2Long)
        .applyTransform(HllCount.Init.forLongs().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .map(Long2long)

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, Long)]
    )(implicit koder: Coder[K], voder: Coder[Long]): SCollection[(K, Long)] =
      in.mapValues(long2Long)
        .toKV
        .applyTransform(HllCount.Init.forLongs().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)
  }

  case class ZetasketchHllStringCounter(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[String] {

    override def estimateDistinctCount(in: SCollection[String]): SCollection[Long] =
      in.applyTransform(HllCount.Init.forStrings().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .map(Long2long)

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, String)]
    )(implicit koder: Coder[K], voder: Coder[String]): SCollection[(K, Long)] =
      in.toKV
        .applyTransform(HllCount.Init.forStrings().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)
  }

  case class ZetasketchHllByteArrayCounter(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[Array[Byte]] {

    override def estimateDistinctCount(in: SCollection[Array[Byte]]): SCollection[Long] =
      in.applyTransform(HllCount.Init.forBytes().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .map(Long2long)

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, Array[Byte])]
    )(implicit koder: Coder[K], voder: Coder[Array[Byte]]): SCollection[(K, Long)] =
      in.toKV
        .applyTransform(HllCount.Init.forBytes().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)

  }

}
