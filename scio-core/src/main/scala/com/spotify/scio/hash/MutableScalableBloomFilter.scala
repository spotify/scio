package com.spotify.scio.hash

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.google.common.{hash => g}

/**
 * A mutable, scalable wrapper around a Guava [[com.google.common.hash.BloomFilter BloomFilter]]
 *
 * Scalable bloom filters use a series of bloom filters, adding a new one and scaling its size by `growthRate`
 * once the previous filter is saturated in order to maintain the desired false positive probability `fpProb`.
 * A scalable bloom filter `contains` ("might contain") an item if any of its filters contains the item.
 *
 * Import `magnolify.guava.auto._` to get common instances of Guava [[com.google.common.hash.Funnel Funnel]]s.
 */
object MutableScalableBloomFilter {

  /**
   * The default parameter values for this implementation are based on the findings in "Scalable Bloom Filters",
   * Almeida, Baquero, et al.: http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
   *
   * @param initialCapacity   The capacity of the first filter
   * @param fpProb            The desired overall false positive probability
   * @param growthRate        The growth rate of each subsequent filter added to `filters`
   * @param tighteningRatio   The tightening ratio applied to the current `fpProb` to maintain the false positive probability over the sequence of filters
   * @tparam T                The type of objects inserted into the filter
   * @return  A scalable bloom filter
   */
  def apply[T: g.Funnel](
    initialCapacity: Long,
    fpProb: Double = 0.03,
    growthRate: Int = 2,
    tighteningRatio: Double = 0.9
  ): MutableScalableBloomFilter[T] = MutableScalableBloomFilter(fpProb, initialCapacity, growthRate, tighteningRatio, fpProb, 0L, Nil)

  def toBytes[T](sbf: MutableScalableBloomFilter[T]): Array[Byte] = {
    // serialize each of the fields, excepting the implicit funnel
    val baos = new ByteArrayOutputStream()
    val dos: DataOutputStream = new DataOutputStream(baos)

    dos.writeDouble(sbf.fpProb)
    dos.writeLong(sbf.headCapacity)
    dos.writeInt(sbf.growthRate)
    dos.writeDouble(sbf.tighteningRatio)
    dos.writeDouble(sbf.headFPProb)
    dos.writeLong(sbf.headCount)
    dos.writeInt(sbf.filters.size)  // num filters
    sbf.filters.foreach { filter => filter.writeTo(dos) }
    baos.toByteArray
  }

  def fromBytes[T](bytes: Array[Byte])(implicit funnel: g.Funnel[T]): MutableScalableBloomFilter[T] = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)

    val fpProb = dis.readDouble()
    val headCapacity = dis.readLong()
    val growthRate = dis.readInt()
    val tighteningRatio = dis.readDouble()
    val headFPProb = dis.readDouble()
    val headCount = dis.readLong()
    val numFilters = dis.readInt()
    val filters = (1 to numFilters).map { _ => g.BloomFilter.readFrom[T](dis, funnel) }.toList

    MutableScalableBloomFilter[T](fpProb, headCapacity, growthRate, tighteningRatio, headFPProb, headCount, filters)
  }
}

/**
 * @param fpProb            The desired false positive probability
 * @param headCapacity      The capacity of the filter at the head of `filters`
 * @param growthRate        The growth rate of each subsequent filter added to `filters`
 * @param tighteningRatio   The tightening ratio applied to the current `fpProb` to maintain the false positive probability over the sequence of filters
 * @param filters           The underlying 'plain' bloom filters
 * @param headFPProb        The false positive probability of the head of `filters`
 * @param headCount         The number of items currently in the filter at the head of `filters`
 * @param funnel            The funnel to turn `T`s into bytes
 * @tparam T                The type of objects inserted into the filter
 */
case class MutableScalableBloomFilter[T](
  fpProb: Double,
  private var headCapacity: Long,
  private val growthRate: Int,
  private val tighteningRatio: Double,
  private var headFPProb: Double,
  // storing a count of items in the head avoids calling the relatively expensive `approximateElementCount` after each insert
  private var headCount: Long,
  // package private for testing purposes
  private[hash] var filters: List[g.BloomFilter[T]]
)(implicit private val funnel: g.Funnel[T])
    extends Serializable {
  def contains(item: T): Boolean = filters.exists(f => f.mightContain(item))
  def approximateElementCount: Long = filters.iterator.map(_.approximateElementCount).sum
  private[hash] def numFilters: Int = filters.size

  private def scale(): Unit = {
    val shouldGrow = headCount >= headCapacity || filters == Nil
    if (shouldGrow) {
      // on construction of the first filter, leave headFPProb & headCapacity at their starting values
      if (filters != Nil) {
        headFPProb = headFPProb * tighteningRatio
        headCapacity = growthRate * headCapacity
      }
      headCount = 0
      filters = g.BloomFilter.create[T](funnel, headCapacity, headFPProb) :: filters
    }
  }

  def +=(item: T): MutableScalableBloomFilter[T] = {
    scale()
    val changed = filters.head.put(item)
    if (changed) headCount = headCount + 1
    this
  }

  def ++=(items: Iterable[T]): MutableScalableBloomFilter[T] = {
    items.foreach(i => this += i) // no bulk insert for guava BFs
    this
  }
}
