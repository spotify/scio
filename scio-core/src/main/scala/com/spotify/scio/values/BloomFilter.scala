package com.spotify.scio.values

import java.io.{ByteArrayOutputStream, InputStream, OutputStream}

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.AtomicCoder

/**
 * An immutable wrapper over Guava's Bloom Filter.
 *
 * Serializable as a field in Avro Schema.
 *
 * @see companion object [[BloomFilter]]
 */
case class BloomFilter[T] private (private val internal: gBloomFilter[T]
) extends ApproxFilter[T] {

  def expectedFpp: Double = internal.expectedFpp()

  def mayBeContains(t: T): Boolean = internal.mightContain(t)

  override def serialize: Array[Byte] = {
    val ba = new ByteArrayOutputStream()
    internal.writeTo(ba)
    ba.toByteArray
  }

}

object BloomFilter {

  /**
   * Coder for [[BloomFilter]]
   */
  implicit def coder[T](implicit f: Funnel[T]): Coder[BloomFilter[T]] =
    Coder.beam(
      new AtomicCoder[BloomFilter[T]] {
        override def encode(value: BloomFilter[T], outStream: OutputStream): Unit =
          value.internal.writeTo(outStream)
        override def decode(inStream: InputStream): BloomFilter[T] =
          BloomFilter(gBloomFilter.readFrom(inStream, f))
      }
    )

  /**
   * Constructor for [[BloomFilter]]
   *
   * @param iterable An iterable of elements to be stored in the BloomFilter
   * @param fpp allowed false positive probability
   * @param f [[Funnel]] for the type [[T]]
   * @return A [[BloomFilter]] for the given [[Iterable]] of elements.
   */
  def apply[T](
    iterable: Iterable[T],
    fpProb: Double
  )(
    implicit f: Funnel[T]
  ): BloomFilter[T] = {
    val numElements = iterable.size
    val settings = BloomFilter.optimalBFSettings(numElements, fpProb)
    require(settings.numBFs == 1,
      s"BloomFilter overflow: $numElements elements found, max allowed: ${settings.capacity}")

    val bf = gBloomFilter.create[T](f, iterable.size, fpProb)
    val it = iterable.iterator
    while (it.hasNext) {
      bf.put(it.next())
    }
    BloomFilter(bf)
  }

  /**
   * Constructor for [[BloomFilterBuilder]]
   *
   * A [[BloomFilterBuilder]] is useful for building [[BloomFilter]]s from [[SCollection]]
   *
   * @param fpp expected false positive probability
   */
  def apply[T: Funnel](fpp: Double): BloomFilterBuilder[T] =
    new BloomFilterBuilder(fpp)

  /**
   * Build the bloom filter in parallel (using a monoid aggregator)
   */
  def par[T: Funnel: Coder](numElements: Int, fpp: Double): BloomFilterParallelBuilder[T] =
    new BloomFilterParallelBuilder(numElements, fpp)


  private[values] final case class BFSettings(width: Int, capacity: Int, numBFs: Int)

  /*
   * This function calculates the width and number of bloom filters that would be optimally
   * required to maintain the given fpProb.
   *
   * TODO reuse this for pair scoll functions
   */
  private[values] def optimalBFSettings(numEntries: Long, fpProb: Double): BFSettings = {
    // double to int rounding error happens when numEntries > (1 << 27)
    // set numEntries upper bound to 1 << 27 to avoid high false positive
    def estimateWidth(numEntries: Int, fpProb: Double): Int =
      math
        .ceil(-1 * numEntries * math.log(fpProb) / math.log(2) / math.log(2))
        .toInt

    // upper bound of n as 2^x
    def upper(n: Int): Int = 1 << (0 to 27).find(1 << _ >= n).get

    // cap capacity between [minSize, maxSize] and find upper bound of 2^x
    val (minSize, maxSize) = (2048, 1 << 27)
    var capacity = upper(math.max(math.min(numEntries, maxSize).toInt, minSize))

    // find a width with the given capacity
    var width = estimateWidth(capacity, fpProb)
    while (width == Int.MaxValue) {
      capacity = capacity >> 1
      width = estimateWidth(capacity, fpProb)
    }
    val numBFs = (numEntries / capacity).toInt + 1

    val totalBytes = width.toLong * numBFs / 8
    val totalSizeMb = totalBytes / 1024.0 / 1024.0

    BFSettings(width, capacity, numBFs)
  }

}
