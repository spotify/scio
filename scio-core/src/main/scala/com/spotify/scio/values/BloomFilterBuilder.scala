package com.spotify.scio.values
import java.io.ByteArrayInputStream

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.coders.Coder

/**
 * Builders for Bloom Filters.
 *
 * These builders define the various ways in which a BloomFilter can be constructed from
 * a SCollection
 */
class BloomFilterBuilder[T: Funnel](fpProb: Double) extends ApproxFilterBuilder[T, BloomFilter] {

  override def build(it: Iterable[T]): BloomFilter[T] = {
    val numElements = it.size
    val settings = BloomFilter.optimalBFSettings(numElements, fpProb)
    require(settings.numBFs == 1,
            s"BloomFilter overflow: $numElements elements found, max allowed: ${settings.capacity}")
    BloomFilter.apply(it, fpProb)
  }

  override def fromBytes(serializedBytes: Array[Byte]): BloomFilter[T] = {
    val inStream = new ByteArrayInputStream(serializedBytes)
    BloomFilter(gBloomFilter.readFrom(inStream, implicitly[Funnel[T]]))
  }

}

/**
 * Build [[BloomFilter]] in parallel from an [[SCollection]]
 *
 * Useful when we know an approxNumber of Elements.
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
