/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.util

/**
 * An implementation of a mutable bloom filter Algebird monoid and aggregator.
 *
 * This is incompatible with Algebird's implementation of BloomFilter, and cannot be converted
 * to Algebird bloom filter because it uses a different implementation of hashing underneath.
 *
 * Upstream PR in Algebird / BloomFilter Benchmark results:
 * https://github.com/twitter/algebird/pull/673/
 *
 * Why do we use mutable BloomFilter:
 * A BloomFilter is modelled as a BoundedSemilattice which means they are commutative semigroups
 * (i.e. combine) whose operation are also idempotent. In Scio the best known usage is where we
 * create bloom filters from a SCollection to filter elements before a join for cases where elements
 * of one SCollection is Sparse in the other. Idempotency of the data structure means that if there
 * are failures in intermediate steps while creating a BloomFilterMonoid, and they are retried, they
 * won't result in an invalid BloomFilter. We also never need to access a BloomFilter which has
 * a limited number of elements from an SCollection. This Bloom Filter should only used to go from
 * SCollection[T] => SCollection[BFMonoid[T]/] with only one BloomFilter.
 *
 */
import java.util

import algebra.BoundedSemilattice
import com.twitter.algebird.{
  Approximate,
  ApproximateBoolean,
  Hash128,
  Monoid,
  MonoidAggregator,
  BloomFilter => AlgebirdImmutableBloomFilter
}

import scala.collection.mutable

/**
 * Helpers for creating Mutable Bloom Filters.
 *
 * Most implementations for estimations are same as Algebird Immutable Bloom Filters
 * and these functions are aliases for them.
 */
object BloomFilter {

  def apply[A](numEntries: Int, fpProb: Double)(implicit hash: Hash128[A]): BloomFilterMonoid[A] =
    optimalWidth(numEntries, fpProb) match {
      case None =>
        throw new java.lang.IllegalArgumentException(
          s"BloomFilter cannot guarantee the specified false positive probability " +
            s"for the number of entries! (numEntries: $numEntries, fpProb: $fpProb)")
      case Some(width) =>
        val numHashes = optimalNumHashes(numEntries, width)
        BloomFilterMonoid[A](numHashes, width)(hash)
    }

  // Mostly an alias to actual functions defined for Algebird's Immutable Bloom Filters.
  def optimalNumHashes(numEntries: Int, width: Int): Int =
    AlgebirdImmutableBloomFilter.optimalNumHashes(numEntries, width)

  def optimalWidth(numEntries: Int, fpProb: Double): Option[Int] =
    AlgebirdImmutableBloomFilter.optimalWidth(numEntries, fpProb)

  /**
   * Cardinality estimates are taken from Theorem 1 on page 15 of
   * "Cardinality estimation and dynamic length adaptation for Bloom filters"
   * by Papapetrou, Siberski, and Nejdl:
   * http://www.softnet.tuc.gr/~papapetrou/publications/Bloomfilters-DAPD.pdf
   *
   * Roughly, by using bounds on the expected number of true bits after n elements
   * have been inserted into the Bloom filter, we can go from the actual number of
   * true bits (which is known) to an estimate of the cardinality.
   *
   * approximationWidth defines an interval around the maximum-likelihood cardinality
   * estimate. Namely, the approximation returned is of the form
   * (min, estimate, max) =
   * ((1 - approxWidth) * estimate, estimate, (1 + approxWidth) * estimate)
   */
  def sizeEstimate(numBits: Int,
                   numHashes: Int,
                   width: Int,
                   approximationWidth: Double = 0.05): Approximate[Long] =
    AlgebirdImmutableBloomFilter.sizeEstimate(numBits, numHashes, width, approximationWidth)

}

/**
 * Bloom Filter - a probabilistic data structure to test presence of an element.
 *
 * Operations
 * 1) insert: hash the value k times, updating the bitfield at the index equal to each hashed value
 * 2) query: hash the value k times.  If there are k collisions, then return true; otherwise false.
 *
 * http://en.wikipedia.org/wiki/Bloom_filter
 *
 * This implementation of the BloomFilterMonoid is mutable and adding elements changes the
 * filter. This is particularly useful when a filter needs to be created for a large number (>1M)
 * of elements at once and fast.
 *
 */
case class BloomFilterMonoid[A](numHashes: Int, width: Int)(implicit hash: Hash128[A])
    extends Monoid[MutableBF[A]]
    with BoundedSemilattice[MutableBF[A]] {

  val hashes: KirMit32Hash[A] = KirMit32Hash[A](numHashes, width)(hash)

  val zero: MutableBF[A] = MutableBFZero[A](hashes, width)

  /**
   * Adds the Bloom Filter on right to the left, mutating and returning Left.
   * Assume that both have the same number of hashes and width.
   */
  override def plus(left: MutableBF[A], right: MutableBF[A]): MutableBF[A] =
    left ++= right

  override def sumOption(as: TraversableOnce[MutableBF[A]]): Option[MutableBF[A]] =
    if (as.isEmpty) {
      None
    } else {
      // We start with an empty instance here.
      // An empty instance is always Sparse, and doesn't allocate the complete
      // memory of the underlying bit map. When adding BFs slowly, at one point
      // it is no longer sparse and hence `++=` returns a BF with the underlying
      // bitmap allocated. For later operations we must hold on to the new BF,
      // and hence we use var here, even though `++=` mutates the instance.
      var outputInstance = MutableBFInstance.empty(hashes, width)
      as.foreach { bf =>
        outputInstance = outputInstance ++= bf
      }
      if (outputInstance.numBits == 0) {
        Some(MutableBFZero(hashes, width))
      } else {
        Some(outputInstance)
      }
    }

  /**
   * Create a bloom filter with one item.
   */
  def create(item: A): MutableBF[A] = MutableBFInstance(hashes, width, item)

  /**
   * Create a bloom filter with multiple items.
   */
  def create(data: A*): MutableBF[A] = create(data.iterator)

  /**
   * Create a bloom filter with multiple items from an iterator
   */
  def create(data: Iterator[A]): MutableBF[A] = {
    val outputInstance = MutableBFInstance.empty(hashes, width)
    data.foreach { itm =>
      outputInstance += itm
    }
    outputInstance
  }

}

object MutableBF {
  implicit def equiv[A]: Equiv[MutableBF[A]] =
    new Equiv[MutableBF[A]] {
      def equiv(a: MutableBF[A], b: MutableBF[A]): Boolean =
        (a eq b) || ((a.numHashes == b.numHashes) &&
          (a.width == b.width) &&
          a.toBitSet.equals(b.toBitSet))
    }
}

/**
 * A Mutable Bloom Filter data structure
 */
sealed abstract class MutableBF[A] extends java.io.Serializable {
  def numHashes: Int

  def width: Int

  /**
   * The number of bits set to true in the bloom filter
   */
  def numBits: Int

  /**
   * Proportion of bits that are set to true.
   */
  def density: Double = numBits.toDouble / width

  // scalastyle:off method.name
  def ++=(other: MutableBF[A]): MutableBF[A]

  def +=(other: A): MutableBF[A]
  // scalastyle:on method.name

  def checkAndAdd(item: A): (MutableBF[A], ApproximateBoolean)

  def contains(item: A): ApproximateBoolean =
    if (maybeContains(item)) {
      // The false positive probability (the probability that the Bloom filter erroneously
      // claims that an element x is in the set when x is not) is roughly
      // p = (1 - e^(-numHashes * setCardinality / width))^numHashes
      // See: http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
      //
      // However, the true set cardinality may not be known. From empirical evidence, though,
      // it is upper bounded with high probability by 1.1 * estimatedCardinality (as long as the
      // Bloom filter is not too full), so we plug this into the formula instead.
      // TODO: investigate this upper bound and density more closely (or derive a better formula).
      // TODO: The following logic is same for immutable Bloom Filters and may be referred here.
      val fpProb =
        if (density > 0.95)
          1.0 // No confidence in the upper bound on cardinality.
        else
          scala.math.pow(1 - scala.math.exp(-numHashes * size.estimate * 1.1 / width), numHashes)

      ApproximateBoolean(true, 1 - fpProb)
    } else {
      // False negatives are not possible.
      ApproximateBoolean.exactFalse
    }

  /**
   * This may be faster if you don't care about evaluating
   * the false positive probability
   */
  def maybeContains(item: A): Boolean

  // Estimates the cardinality of the set of elements that have been
  // inserted into the Bloom Filter.
  def size: Approximate[Long]

  def toBitSet: util.BitSet

  def copy: MutableBF[A]

  /**
   * Compute the Hamming distance between the two Bloom filters
   * `a` and `b`. The distance is defined as the number of bits that
   * need to change to in order to transform one filter into the other.
   * This is computed using XOR but it doesn't mutate any BloomFilters
   */
  def hammingDistance(that: MutableBF[A]): Int =
    (this, that) match {
      // Comparing with empty filter should give number
      // of bits in other set
      case (x: MutableBFZero[A], y: MutableBFZero[A]) => 0
      case (x: MutableBFZero[A], y: MutableBF[A])     => y.numBits
      case (x: MutableBF[A], y: MutableBFZero[A])     => x.numBits

      // Otherwise compare as bit sets
      case (_, _) =>
        // hammingDistance should not mutate BloomFilter
        val thisCopy = this.toBitSet.clone().asInstanceOf[util.BitSet]
        thisCopy.xor(that.toBitSet)
        thisCopy.cardinality()
    }

}

/**
 * Empty bloom filter.
 */
final case class MutableBFZero[A](hashes: KirMit32Hash[A], width: Int) extends MutableBF[A] {

  def toBitSet: util.BitSet = new util.BitSet()

  def numHashes: Int = hashes.size

  def numBits: Int = 0

  // scalastyle:off method.name
  def ++=(other: MutableBF[A]): MutableBF[A] = other

  def +=(other: A): MutableBF[A] = MutableBFInstance[A](hashes, width, other)
  // scalastyle:on method.name

  def checkAndAdd(other: A): (MutableBF[A], ApproximateBoolean) =
    (this += other, ApproximateBoolean.exactFalse)

  override def contains(item: A): ApproximateBoolean = ApproximateBoolean.exactFalse

  def maybeContains(item: A): Boolean = false

  def size: Approximate[Long] = Approximate.exact[Long](0)

  def copy: MutableBF[A] = MutableBFZero(hashes, width)
}

/**
 * Mutable Bloom filter with multiple values
 */
final case class MutableBFInstance[A](hashes: KirMit32Hash[A], bits: util.BitSet, width: Int)
    extends MutableBF[A] {

  def numHashes: Int = hashes.size

  /**
   * The number of bits set to true
   */
  def numBits: Int = bits.cardinality()

  def toBitSet: util.BitSet = bits

  // scalastyle:off method.name
  def ++=(other: MutableBF[A]): MutableBF[A] = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case MutableBFZero(_, _)                         => this
      case MutableSparseBFInstance(_, otherSetBits, _) =>
        // This is MutableBFInstance, hence not sparse, so don't convert output to sparse.
        val it = otherSetBits.flatten.iterator
        // OR operation, without allocating otherSetBits as util.BitSet
        while (it.hasNext) {
          bits.set(it.next())
        }
        this
      case MutableBFInstance(_, otherBits, _) =>
        bits.or(otherBits)
        this
    }
  }

  def +=(item: A): MutableBF[A] = {
    val itemHashes = hashes(item)
    itemHashes.foreach(bits.set)
    this
  }
  // scalastyle:on method.name

  def checkAndAdd(other: A): (MutableBF[A], ApproximateBoolean) = {
    val doesContain = contains(other)
    (this += other, doesContain)
  }

  // scalastyle:off return
  def maybeContains(item: A): Boolean = {
    val il = hashes(item)
    var idx = 0
    while (idx < il.length) {
      val i = il(idx)
      if (!bits.get(i)) return false
      idx += 1
    }
    true
  }
  // scalastyle:on return

  // use an approximation width of 0.05
  def size: Approximate[Long] =
    BloomFilter.sizeEstimate(numBits, numHashes, width, 0.05)

  def copy: MutableBF[A] = MutableBFInstance(hashes, bits.clone.asInstanceOf[util.BitSet], width)
}

/**
 * Mutable SparseBloomFilter or a Delayed MutableBFInstance.
 * If the underlying bit set is less than 1/32 filled, it stores the actual hashes calculated
 * instead of allocating memory for the complete BitSet
 *
 * After adding enough elements when the size of the underlying Set becomes
 * more than 32 * numBits, it allocates memory for a BitSet and creates a MutableBFInstance.
 *
 * If a BitSet with a width of 'w' has very few elements it still allocates memory to store
 * bits from 0 to w-1. This method is a workaround for that.
 *
 * EWAHCompressedBitmap is not used because OR operations are immutable and copies the underlying
 * bitmap. Also Apache Beam doesn't have a Coder for EWAHCompressedBitmap, and it would fallback
 * to Kryo
 */
final case class MutableSparseBFInstance[A](hashes: KirMit32Hash[A],
                                            allHashes: mutable.Buffer[Array[Int]],
                                            width: Int)
    extends MutableBF[A] {

  def numHashes: Int = hashes.size

  /**
   * The number of bits set to true
   */
  def numBits: Int = setBits.size

  private def allSeenBit: mutable.Buffer[Int] = allHashes.flatten

  private var set: Set[Int] = _
  // Keeps a state if elements were added after `allHashes` was converted into a set.
  private var setIsStale: Boolean = true

  // Access all the set bits as a set. This is meant to be used by maybeContains
  // The value is cached so that it is a set is created only once.
  // This cannot be a lazy val, because it is updated when an element gets added.
  private def setBits: Set[Int] = {
    if (setIsStale) {
      set = allSeenBit.toSet
      setIsStale = false
    }
    set
  }

  def toBitSet: util.BitSet = {
    val jbitSet = new util.BitSet()
    allSeenBit // setBits would involve an additional hashing operation to convert to a HashSet.
      .foreach(jbitSet.set)
    jbitSet
  }

  /**
   * Convert to a MutableBFInstance backed by an actual BitSet instead of storing indexes in a Set.
   */
  private def asMutableBFInstance = MutableBFInstance(hashes, toBitSet, width)

  // scalastyle:off method.name
  def ++=(other: MutableBF[A]): MutableBF[A] = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case MutableBFZero(_, _) => this
      case MutableSparseBFInstance(_, otherSetBits, _) =>
        setIsStale = true
        if ((allHashes.size + otherSetBits.size) * numHashes * 32 >= width) {
          // TODO this will work with no hash Collition. can we do better?
          // We mutate this (MutableSparseBFInstance) but return a MutableBFInstance
          // This makes sure we follow the contract of a ++= and mutate this,
          // and we move from sparse to non sparse once the size exceeds some.
          allHashes ++= otherSetBits
          // convert to MutableBFInstance
          asMutableBFInstance ++= other
        } else {
          // stay sparse
          allHashes ++= otherSetBits
          this
        }
      case MutableBFInstance(_, otherBits, _) =>
        setIsStale = true
        // since the other is not a sparse BF, the result cannot be sparse.
        asMutableBFInstance ++= other
    }
  }

  def +=(item: A): MutableBF[A] = {
    setIsStale = true
    val itemHashes = hashes(item)
    allHashes += itemHashes
    this
  }
  // scalastyle:on method.name

  def checkAndAdd(other: A): (MutableBF[A], ApproximateBoolean) = {
    val doesContain = contains(other)
    (this += other, doesContain)
  }

  // scalastyle:off return
  def maybeContains(item: A): Boolean = {
    val sb = setBits // eval setBits only once.
    val il = hashes(item)
    var idx = 0
    while (idx < il.length) {
      val i = il(idx)
      if (!sb.contains(i)) return false
      idx += 1
    }
    true
  }
  // scalastyle:on return

  // use an approximation width of 0.05
  def size: Approximate[Long] =
    BloomFilter.sizeEstimate(numBits, numHashes, width, 0.05)

  def copy: MutableBF[A] = MutableSparseBFInstance(hashes, allHashes.clone, width)
}

/**
 * Constructors for mutable bloom filters
 */
object MutableBFInstance {
  def apply[A](hashes: KirMit32Hash[A], width: Int, firstElement: A): MutableBF[A] = {
    val bf = MutableBFInstance.empty(hashes, width)
    bf += firstElement
  }

  def apply[A](hashes: KirMit32Hash[A], width: Int): MutableBF[A] =
    empty(hashes, width)

  // Always Start with a Sparse BF Instance
  def empty[A](hashes: KirMit32Hash[A], width: Int): MutableBF[A] =
    MutableSparseBFInstance(hashes, mutable.Buffer.empty[Array[Int]], width)
}

/**
 * Logic for creating `n` hashes for each item of the BloomFilter.
 *
 * The hashing strategy used here are different than the one used for
 * [[com.twitter.algebird.BloomFilter]] and hence a BloomFilter with this strategy
 * is incompatible with the one from Algebird and cannot be converted to that.
 *
 * This hash function derivation is explained by Adam Kirsch and Michael Mitzenmacher here:
 * https://www.eecs.harvard.edu/~michaelm/postscripts/esa2006a.pdf
 * which argues that this trick uses two 32 bit hashes to find numHashes without causing significant
 * deterioration in performance.
 *
 * We have noticed 2 to 4 times higher throughput when using this approach compared to the
 * implementation in Algebird.
 */
final case class KirMit32Hash[A](numHashes: Int, width: Int)(implicit hash128: Hash128[A]) {
  val size: Int = numHashes

  def apply(valueToHash: A): Array[Int] = {
    val (hash64_1, hash_64_2) = hash128.hashWithSeed(numHashes, valueToHash)
    // We just need two 32 bit hashes. So just convert toInt, and ignore the rest.
    val (hash1, hash2) = (hash64_1.toInt, hash_64_2.toInt)

    val hashes = new Array[Int](numHashes)
    var i = 0
    while (i < numHashes) {
      // math.abs of a % would never return -ve
      hashes(i) = math.abs((hash1 + i * hash2) % width)
      i += 1
    }
    hashes
  }
}

case class BloomFilterAggregator[A](bfMonoid: BloomFilterMonoid[A])
    extends MonoidAggregator[A, MutableBF[A], MutableBF[A]] {
  val monoid = bfMonoid

  def prepare(value: A): MutableBF[A] = monoid.create(value)

  def present(bf: MutableBF[A]): MutableBF[A] = bf
}

object BloomFilterAggregator {
  def apply[A](numHashes: Int, width: Int)(implicit hash: Hash128[A]): BloomFilterAggregator[A] =
    BloomFilterAggregator[A](BloomFilterMonoid[A](numHashes, width))
}
