package com.spotify.scio.values
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
    approxFilterCoder: Coder[BloomFilter[T]]
  ): SCollection[BloomFilter[T]] = {
    if (sc.context.isTest) { // TODO Explain this override // Fix this once we know what is wrong with aggregate
      super.build(sc)
    } else {
      sc.withName("Build Bloom Filter")
        .aggregate(zeroValue = gBloomFilter.create(implicitly[Funnel[T]], numElements, fpProb))(
          seqOp = (gbf, t) => {
            gbf.put(t)
            gbf
          },
          combOp = (bf1, bf2) => {
            if (bf1 != bf2) bf1.putAll(bf2)
            bf1
          }
        )
        .map(BloomFilter(_))
    }
  }
}
