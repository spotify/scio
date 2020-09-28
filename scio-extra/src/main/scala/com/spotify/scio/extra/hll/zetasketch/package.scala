package com.spotify.scio.extra.hll

import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.Functions
import org.apache.beam.sdk.extensions.zetasketch.HllCount
import org.apache.beam.sdk.transforms.{MapElements, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

package object zetasketch {

  case class ZetasketchHllCountInt(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[Int] {

    override def estimateDistinctCount(in: PCollection[Int]): PCollection[java.lang.Long] =
      in.apply(MapElements.via(Functions.simpleFn(int2Integer)))
        .apply(HllCount.Init.forIntegers().withPrecision(p).globally())
        .apply(HllCount.Extract.globally())

    override def estimateDistinctPerKey[K](
      in: PCollection[KV[K, Int]]
    ): PCollection[KV[K, java.lang.Long]] =
      in
        .apply(
          ParDo.of(
            Functions.mapFn[KV[K, Int], KV[K, Integer]](kv =>
              KV.of(kv.getKey, int2Integer(kv.getValue))
            )
          )
        )
        .apply(HllCount.Init.forIntegers().withPrecision(p).perKey())
        .apply(HllCount.Extract.perKey())
  }
}
