package com.spotify.scio.values
import java.io.InputStream

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.coders.Coder

/**
 * Builders for [[BloomFilter]]
 *
 * These builders define the various ways in which a BloomFilter can be constructed from
 * a SCollection. Internally a builder can choose to use any implementation (mutable / immutable)
 * to create a [[BloomFilter]]
 */
class BloomFilterBuilder[T: Funnel](fpProb: Double) extends ApproxFilterBuilder[T, BloomFilter] {

  override def build(it: Iterable[T]): BloomFilter[T] = BloomFilter(it, fpProb)

  override def readFrom(
    in: InputStream
  ): BloomFilter[T] =
    BloomFilter(gBloomFilter.readFrom(in, implicitly[Funnel[T]]))

}

/**
 * Build [[BloomFilter]] in parallel from an [[SCollection]]
 *
 * Useful when we know an approxNumber of Elements
 */
class BloomFilterParallelBuilder[T: Funnel] private[values] (
  numElements: Long,
  fpProb: Double
) extends BloomFilterBuilder[T](fpProb) {

  require(
    BloomFilter.optimalBFSettings(numElements, fpProb).numBFs == 1,
    s"Cannot store $numElements elements in one BloomFilter"
  )

  override def build(sc: SCollection[T])(
    implicit coder: Coder[T],
    approxFilterCoder: Coder[BloomFilter[T]]): SCollection[BloomFilter[T]] = {
    sc.aggregate(zeroValue = gBloomFilter.create(implicitly[Funnel[T]], numElements, fpProb))(
        seqOp = (gbf, t) => {
          gbf.put(t)
          gbf
        },
        combOp = (bf1, bf2) => {
          bf1.putAll(bf2)
          bf1
        }
      )
      .map(BloomFilter(_))
  }
}
