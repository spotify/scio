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
private[values] abstract class BloomFilterBuilder[T: Funnel]
    extends ApproxFilterBuilder[T, BloomFilter] {

  override def fromBytes(serializedBytes: Array[Byte]): BloomFilter[T] = {
    val inStream = new ByteArrayInputStream(serializedBytes)
    BloomFilter(gBloomFilter.readFrom(inStream, implicitly[Funnel[T]]))
  }

}

case class SingleThreadedBloomFilterBuilder[T: Funnel: Coder] private[values] (fpp: Double)
    extends BloomFilterBuilder[T] {

  override def build(sc: SCollection[T]): SCollection[BloomFilter[T]] =
    sc.groupBy(_ => ())
      .values
      .map { it =>
        val numElements = it.size
        val settings = BloomFilter.optimalBFSettings(numElements, fpp)
        require(
          settings.numBFs == 1,
          s"BloomFilter overflow: $numElements elements found, max allowed: ${settings.capacity}")
        BloomFilter.apply(it, fpp)
      }(BloomFilter.coder[T])
}

/**
 * Build [[BloomFilter]] in parallel from an [[SCollection]]
 *
 * Useful when we know an approxNumber of Elements.
 */
case class BloomFilterParallelBuilder[T: Funnel: Coder] private[values] (
  numElements: Long,
  fpp: Double
) extends BloomFilterBuilder[T] {

  require(
    BloomFilter.optimalBFSettings(numElements, fpp).numBFs == 1,
    s"Cannot store $numElements elements in one BloomFilter"
  )

  override def build(sc: SCollection[T]): SCollection[BloomFilter[T]] = {
    sc.aggregate(zeroValue = gBloomFilter.create(implicitly[Funnel[T]], numElements, fpp))(
        seqOp = (gbf, t) => {
          gbf.put(t)
          gbf
        },
        combOp = (bf1, bf2) => {
          bf1.putAll(bf2)
          bf1
        }
      )
      .map(BloomFilter(_))(BloomFilter.coder[T])
  }
}
