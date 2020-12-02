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

package com.spotify.scio.values

import com.google.common.hash.Funnel
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{BeamCoders, Coder, CoderMaterializer}
import com.spotify.scio.estimators.{
  ApproxDistinctCounter,
  ApproximateUniqueCounter,
  ApproximateUniqueCounterByError
}
import com.spotify.scio.hash._
import com.spotify.scio.util._
import com.spotify.scio.util.random.{BernoulliValueSampler, PoissonValueSampler}
import com.spotify.scio.values.SCollection.makePairSCollectionFunctions
import com.twitter.algebird.{Aggregator, Monoid, MonoidAggregator, Semigroup}
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, PCollection}
import org.slf4j.LoggerFactory

import scala.collection.compat.immutable.ArraySeq

private object PairSCollectionFunctions {
  private val logger = LoggerFactory.getLogger(this.getClass)
}

/**
 * Extra functions available on SCollections of (key, value) pairs through an implicit conversion.
 *
 * @groupname cogroup CoGroup Operations
 * @groupname join Join Operations
 * @groupname per_key Per Key Aggregations
 * @groupname transform Transformations
 */
class PairSCollectionFunctions[K, V](val self: SCollection[(K, V)]) {
  import TupleFunctions._

  private[this] val context: ScioContext = self.context

  implicit val (keyCoder: Coder[K], valueCoder: Coder[V]) = BeamCoders.getTupleCoders(self)

  private[scio] def toKV: SCollection[KV[K, V]] =
    self.map(kv => KV.of(kv._1, kv._2))(Coder.raw(CoderMaterializer.kvCoder[K, V](context)))

  private[values] def applyPerKey[UI: Coder, UO: Coder](
    t: PTransform[_ >: PCollection[KV[K, V]], PCollection[KV[K, UI]]]
  )(f: KV[K, UI] => (K, UO)): SCollection[(K, UO)] = {
    self.transform(
      _.withName("TupleToKv").toKV
        .applyTransform(t)(Coder.raw(CoderMaterializer.kvCoder[K, UI](context)))
        .withName("KvToTuple")
        .map(f)
    )
  }

  /**
   * Apply a [[org.apache.beam.sdk.transforms.DoFn DoFn]] that processes [[KV]]s and wrap the
   * output in an [[SCollection]].
   */
  def applyPerKeyDoFn[U: Coder](t: DoFn[KV[K, V], KV[K, U]]): SCollection[(K, U)] =
    this.applyPerKey(ParDo.of(t))(kvToTuple)

  /**
   * Convert this SCollection to an [[SCollectionWithHotKeyFanout]] that uses an intermediate node
   * to combine "hot" keys partially before performing the full combine.
   * @param hotKeyFanout a function from keys to an integer N, where the key will be spread among
   * N intermediate nodes for partial combining. If N is less than or equal to 1, this key will
   * not be sent through an intermediate node.
   */
  def withHotKeyFanout(hotKeyFanout: K => Int): SCollectionWithHotKeyFanout[K, V] =
    new SCollectionWithHotKeyFanout(context, this, Left(hotKeyFanout))

  /**
   * Convert this SCollection to an [[SCollectionWithHotKeyFanout]] that uses an intermediate node
   * to combine "hot" keys partially before performing the full combine.
   * @param hotKeyFanout constant value for every key
   */
  def withHotKeyFanout(hotKeyFanout: Int): SCollectionWithHotKeyFanout[K, V] =
    new SCollectionWithHotKeyFanout(context, this, Right(hotKeyFanout))

  // =======================================================================
  // CoGroups
  // =======================================================================

  /**
   * For each key k in `this` or `rhs`, return a resulting SCollection that contains a tuple with
   * the list of values for that key in `this` as well as `rhs`.
   * @group cogroup
   */
  def cogroup[W](rhs: SCollection[(K, W)]): SCollection[(K, (Iterable[V], Iterable[W]))] =
    ArtisanJoin.cogroup(self.tfName, self, rhs)

  /**
   * For each key k in `this` or `rhs1` or `rhs2`, return a resulting SCollection that contains
   * a tuple with the list of values for that key in `this`, `rhs1` and `rhs2`.
   * @group cogroup
   */
  def cogroup[W1, W2](
    rhs1: SCollection[(K, W1)],
    rhs2: SCollection[(K, W2)]
  ): SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    MultiJoin.withName(self.tfName).cogroup(self, rhs1, rhs2)

  /**
   * For each key k in `this` or `rhs1` or `rhs2` or `rhs3`, return a resulting SCollection
   * that contains a tuple with the list of values for that key in `this`, `rhs1`, `rhs2` and
   * `rhs3`.
   * @group cogroup
   */
  def cogroup[W1, W2, W3](
    rhs1: SCollection[(K, W1)],
    rhs2: SCollection[(K, W2)],
    rhs3: SCollection[(K, W3)]
  ): SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    MultiJoin.withName(self.tfName).cogroup(self, rhs1, rhs2, rhs3)

  /**
   * Alias for `cogroup`.
   * @group cogroup
   */
  def groupWith[W](rhs: SCollection[(K, W)]): SCollection[(K, (Iterable[V], Iterable[W]))] =
    this.cogroup(rhs)

  /**
   * Alias for `cogroup`.
   * @group cogroup
   */
  def groupWith[W1, W2](
    rhs1: SCollection[(K, W1)],
    rhs2: SCollection[(K, W2)]
  ): SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    this.cogroup(rhs1, rhs2)

  /**
   * Alias for `cogroup`.
   * @group cogroup
   */
  def groupWith[W1, W2, W3](
    rhs1: SCollection[(K, W1)],
    rhs2: SCollection[(K, W2)],
    rhs3: SCollection[(K, W3)]
  ): SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    this.cogroup(rhs1, rhs2, rhs3)

  /**
   * Partition this SCollection using `K.##` into `n` partitions.
   * Note that K should provide consistent hash code accross different JVM.
   *
   * @param numPartitions number of output partitions
   * @return partitioned SCollections in a `Seq`
   * @group collection
   */
  def hashPartitionByKey(numPartitions: Int): Seq[SCollection[(K, V)]] = {
    val hashCodeFn: K => Int = {
      case key: Array[_] => ArraySeq.unsafeWrapArray(key).##
      case key           => key.##
    }
    self.partition(numPartitions, elem => Math.floorMod(hashCodeFn(elem._1), numPartitions))
  }

  // =======================================================================
  // Joins
  // =======================================================================

  /**
   * Perform a full outer join of `this` and `rhs`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), Some(w))) for w in `rhs`,
   * or the pair (k, (Some(v), None)) if no elements in `rhs` have key k. Similarly, for each
   * element (k, w) in `rhs`, the resulting SCollection will either contain all pairs (k,
   * (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements in
   * `this` have key k.
   * @group join
   */
  def fullOuterJoin[W](rhs: SCollection[(K, W)]): SCollection[(K, (Option[V], Option[W]))] =
    ArtisanJoin.outer(self.tfName, self, rhs)

  /**
   * Return an SCollection containing all pairs of elements with matching keys in `this` and
   * `rhs`. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in
   * `this` and (k, v2) is in `rhs`.
   * @group join
   */
  def join[W](rhs: SCollection[(K, W)]): SCollection[(K, (V, W))] =
    ArtisanJoin(self.tfName, self, rhs)

  /**
   * Perform a left outer join of `this` and `rhs`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (v, Some(w))) for w in `rhs`, or the
   * pair (k, (v, None)) if no elements in `rhs` have key k.
   * @group join
   */
  def leftOuterJoin[W](rhs: SCollection[(K, W)]): SCollection[(K, (V, Option[W]))] =
    ArtisanJoin.left(self.tfName, self, rhs)

  /**
   * Perform a right outer join of `this` and `rhs`. For each element (k, w) in `rhs`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k.
   * @group join
   */
  def rightOuterJoin[W](rhs: SCollection[(K, W)]): SCollection[(K, (Option[V], W))] =
    ArtisanJoin.right(self.tfName, self, rhs)

  /**
   * Full outer join for cases when the left collection (`this`) is much larger than the right
   * collection (`rhs`) which cannot fit in memory, but contains a mostly overlapping set of keys
   * as the left collection, i.e. when the intersection of keys is sparse in the left collection.
   * A Bloom Filter of keys from the right collection (`rhs`) is used to split `this` into 2
   * partitions. Only those with keys in the filter go through the join and the rest are
   * concatenated. This is useful for joining historical aggregates with incremental updates.
   * Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param rhsNumKeys An estimate of the number of keys in the right collection `rhs`.
   *                    This estimate is used to find the size and number of BloomFilters rhs Scio
   *                    would use to split the left collection (`this`) into overlap and
   *                    intersection in a "map" step before an exact join.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   * @param fpProb A fraction in range (0, 1) which would be the accepted false positive
   *               probability when computing the overlap.
   *               Note: having fpProb = 0 doesn't mean that Scio would calculate an exact overlap.
   */
  def sparseFullOuterJoin[W](
    rhs: SCollection[(K, W)],
    rhsNumKeys: Long,
    fpProb: Double = 0.01
  )(implicit funnel: Funnel[K]): SCollection[(K, (Option[V], Option[W]))] = self.transform { me =>
    implicit val wCoder: Coder[W] = rhs.valueCoder
    SCollection.unionAll(
      split(me, rhs, rhsNumKeys, fpProb).map { case (lhsUnique, lhsOverlap, rhs) =>
        val unique = lhsUnique.map(kv => (kv._1, (Option(kv._2), Option.empty[W])))
        unique ++ lhsOverlap.fullOuterJoin(rhs)
      }
    )
  }

  /**
   * Inner join for cases when the left collection (`this`) is much larger than the right
   * collection (`rhs`) which cannot fit in memory, but contains a mostly overlapping set of keys
   * as the left collection, i.e. when the intersection of keys is sparse in the left collection.
   * A Bloom Filter of keys from the right collection (`rhs`) is used to split `this` into 2
   * partitions. Only those with keys in the filter go through the join and the rest are filtered
   * out before the join.
   * Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param rhsNumKeys An estimate of the number of keys in the right collection `rhs`.
   *                    This estimate is used to find the size and number of BloomFilters that Scio
   *                    would use to split the left collection (`this`) into overlap and
   *                    intersection in a "map" step before an exact join.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   * @param fpProb A fraction in range (0, 1) which would be the accepted false positive
   *               probability when computing the overlap.
   *               Note: having fpProb = 0 doesn't mean that Scio would calculate an exact overlap.
   */
  def sparseJoin[W](
    rhs: SCollection[(K, W)],
    rhsNumKeys: Long,
    fpProb: Double = 0.01
  )(implicit funnel: Funnel[K]): SCollection[(K, (V, W))] =
    self.transform { me =>
      implicit val wCoder: Coder[W] = rhs.valueCoder
      SCollection.unionAll(
        split(me, rhs, rhsNumKeys, fpProb).map { case (_, lhsOverlap, rhs) =>
          lhsOverlap.join(rhs)
        }
      )
    }

  /**
   * Left outer join for cases when the left collection (`this`) is much larger than the right
   * collection (`rhs`) which cannot fit in memory, but contains a mostly overlapping set of keys
   * as the left collection, i.e. when the intersection of keys is sparse in the left collection.
   * A Bloom Filter of keys from the right collection (`rhs`) is used to split `this` into 2
   * partitions. Only those with keys in the filter go through the join and the rest are
   * concatenated. This is useful for joining historical aggregates with incremental updates.
   * Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param rhsNumKeys An estimate of the number of keys in the right collection `rhs`.
   *                    This estimate is used to find the size and number of BloomFilters that Scio
   *                    would use to split the left collection (`this`) into overlap and
   *                    intersection in a "map" step before an exact join.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   * @param fpProb A fraction in range (0, 1) which would be the accepted false positive
   *               probability when computing the overlap.
   *               Note: having fpProb = 0 doesn't mean that Scio would calculate an exact overlap.
   */
  def sparseLeftOuterJoin[W](
    rhs: SCollection[(K, W)],
    rhsNumKeys: Long,
    fpProb: Double = 0.01
  )(implicit funnel: Funnel[K]): SCollection[(K, (V, Option[W]))] =
    self.transform { me =>
      implicit val wCoder: Coder[W] = rhs.valueCoder
      SCollection.unionAll(
        split(me, rhs, rhsNumKeys, fpProb).map { case (lhsUnique, lhsOverlap, rhs) =>
          val unique = lhsUnique.map(kv => (kv._1, (kv._2, Option.empty[W])))
          unique ++ lhsOverlap.leftOuterJoin(rhs)
        }
      )
    }

  /**
   * Right outer join for cases when the left collection (`this`) is much larger than the right
   * collection (`rhs`) which cannot fit in memory, but contains a mostly overlapping set of keys
   * as the left collection, i.e. when the intersection of keys is sparse in the left collection.
   * A Bloom Filter of keys from the right collection (`rhs`) is used to split `this` into 2
   * partitions. Only those with keys in the filter go through the join and the rest are
   * concatenated. This is useful for joining historical aggregates with incremental updates.
   * Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param rhsNumKeys An estimate of the number of keys in the right collection `rhs`.
   *                    This estimate is used to find the size and number of BloomFilters that Scio
   *                    would use to split the left collection (`this`) into overlap and
   *                    intersection in a "map" step before an exact join.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   * @param fpProb A fraction in range (0, 1) which would be the accepted false positive
   *               probability when computing the overlap.
   *               Note: having fpProb = 0 doesn't mean that Scio would calculate an exact overlap.
   */
  def sparseRightOuterJoin[W](
    rhs: SCollection[(K, W)],
    rhsNumKeys: Long,
    fpProb: Double = 0.01
  )(implicit funnel: Funnel[K]): SCollection[(K, (Option[V], W))] =
    self.transform { me =>
      implicit val wCoder: Coder[W] = rhs.valueCoder
      SCollection.unionAll(
        split(me, rhs, rhsNumKeys, fpProb).map { case (_, lhsOverlap, rhs) =>
          lhsOverlap.rightOuterJoin(rhs)
        }
      )
    }

  /*
   Internal to PairSCollectionFunctions
   Split up parameter `thisSColl` into
   Seq(
     (KeysUniqueInSelf, KeysOverlappingWith`rhsSColl`, PartOfRHSSColl)
   )
   The number of SCollection tuples in the Seq is based on the number of BloomFilters required to
   maintain the given false positive probability for the split of `thisSColl` into Unique and
   Overlap. This function is used by Sparse Join transforms.
   */
  private def split[W](
    thisSColl: SCollection[(K, V)],
    rhsSColl: SCollection[(K, W)],
    rhsNumKeys: Long,
    fpProb: Double
  )(implicit
    funnel: Funnel[K]
  ): Seq[(SCollection[(K, V)], SCollection[(K, V)], SCollection[(K, W)])] = {
    val rhsBfSIs = BloomFilter.createPartitionedSideInputs(self.keys, rhsNumKeys, fpProb)
    val n = rhsBfSIs.size

    val thisParts = thisSColl.hashPartitionByKey(n)
    val rhsParts = rhsSColl.hashPartitionByKey(n)

    thisParts.zip(rhsParts).zip(rhsBfSIs).map { case ((lhs, rhs), bfsi) =>
      val (lhsUnique, lhsOverlap) = (SideOutput[(K, V)](), SideOutput[(K, V)]())
      val partitionedLhs = lhs
        .withSideInputs(bfsi)
        .transformWithSideOutputs(Seq(lhsUnique, lhsOverlap)) { (e, c) =>
          if (c(bfsi).mightContain(e._1)) {
            lhsOverlap
          } else {
            lhsUnique
          }
        }
      (partitionedLhs(lhsUnique), partitionedLhs(lhsOverlap), rhs)
    }
  }

  /**
   * Look up values from `rhs` where `rhs` is much larger and keys from `this` wont fit in memory,
   * and is sparse in `rhs`. A Bloom Filter of keys in `this` is used to filter out irrelevant keys
   * in `rhs`. This is useful when searching for a limited number of values from one or more very
   * large tables. Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param thisNumKeys An estimate of the number of keys in `this`. This estimate is used to find
   *                    the size and number of BloomFilters that Scio would use to pre-filter
   *                    `rhs` before doing a co-group.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   * @param fpProb A fraction in range (0, 1) which would be the accepted false positive
   *               probability when discarding elements of `rhs` in the pre-filter step.
   */
  def sparseLookup[A](rhs: SCollection[(K, A)], thisNumKeys: Long, fpProb: Double)(implicit
    funnel: Funnel[K]
  ): SCollection[(K, (V, Iterable[A]))] = self.transform { sColl =>
    implicit val aCoder: Coder[A] = rhs.valueCoder
    val selfBfSideInputs = BloomFilter.createPartitionedSideInputs(sColl.keys, thisNumKeys, fpProb)
    val n = selfBfSideInputs.size

    val thisParts = sColl.hashPartitionByKey(n)
    val rhsParts = rhs.hashPartitionByKey(n)

    SCollection.unionAll(
      thisParts
        .zip(selfBfSideInputs)
        .zip(rhsParts)
        .map { case ((lhs, lhsBfSi), rhs1) =>
          lhs
            .cogroup(
              rhs1
                .withSideInputs(lhsBfSi)
                .filter((e, c) => c(lhsBfSi).mightContain(e._1))
                .toSCollection
            )
            .flatMap { case (k, (iV, iA)) => iV.map(v => (k, (v, iA))) }
        }
    )
  }

  /**
   * Look up values from `rhs` where `rhs` is much larger and keys from `this` wont fit in memory,
   * and is sparse in `rhs`. A Bloom Filter of keys in `this` is used to filter out irrelevant keys
   * in `rhs`. This is useful when searching for a limited number of values from one or more very
   * large tables. Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param thisNumKeys An estimate of the number of keys in `this`. This estimate is used to find
   *                    the size and number of BloomFilters that Scio would use to pre-filter
   *                    `rhs` before doing a co-group.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   */
  def sparseLookup[A](rhs: SCollection[(K, A)], thisNumKeys: Long)(implicit
    funnel: Funnel[K]
  ): SCollection[(K, (V, Iterable[A]))] = sparseLookup(rhs, thisNumKeys, 0.01)

  /**
   * Look up values from `rhs` where `rhs` is much larger and keys from `this` wont fit in memory,
   * and is sparse in `rhs`. A Bloom Filter of keys in `this` is used to filter out irrelevant keys
   * in `rhs`. This is useful when searching for a limited number of values from one or more very
   * large tables. Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param thisNumKeys An estimate of the number of keys in `this`. This estimate is used to find
   *                    the size and number of BloomFilters that Scio would use to pre-filter
   *                    `rhs1` and `rhs2` before doing a co-group.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   * @param fpProb A fraction in range (0, 1) which would be the accepted false positive
   *               probability when discarding elements of `rhs1` and `rhs2` in the pre-filter
   *               step.
   */
  def sparseLookup[A, B](
    rhs1: SCollection[(K, A)],
    rhs2: SCollection[(K, B)],
    thisNumKeys: Long,
    fpProb: Double
  )(implicit funnel: Funnel[K]): SCollection[(K, (V, Iterable[A], Iterable[B]))] = self.transform {
    sColl =>
      implicit val aCoder: Coder[A] = rhs1.valueCoder
      implicit val bCoder: Coder[B] = rhs2.valueCoder
      val selfBfSideInputs =
        BloomFilter.createPartitionedSideInputs(sColl.keys, thisNumKeys, fpProb)
      val n = selfBfSideInputs.size

      val thisParts = sColl.hashPartitionByKey(n)
      val rhs1Parts = rhs1.hashPartitionByKey(n)
      val rhs2Parts = rhs2.hashPartitionByKey(n)

      SCollection.unionAll(
        thisParts.zip(selfBfSideInputs).zip(rhs1Parts).zip(rhs2Parts).map {
          case (((lhs, lhsBfSi), rhs1), rhs2) =>
            lhs
              .cogroup(
                rhs1
                  .withSideInputs(lhsBfSi)
                  .filter((e, c) => c(lhsBfSi).mightContain(e._1))
                  .toSCollection,
                rhs2
                  .withSideInputs(lhsBfSi)
                  .filter((e, c) => c(lhsBfSi).mightContain(e._1))
                  .toSCollection
              )
              .flatMap { case (k, (iV, iA, iB)) => iV.map(v => (k, (v, iA, iB))) }
        }
      )
  }

  /**
   * Look up values from `rhs` where `rhs` is much larger and keys from `this` wont fit in memory,
   * and is sparse in `rhs`. A Bloom Filter of keys in `this` is used to filter out irrelevant keys
   * in `rhs`. This is useful when searching for a limited number of values from one or more very
   * large tables. Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param thisNumKeys An estimate of the number of keys in `this`. This estimate is used to find
   *                    the size and number of BloomFilters that Scio would use to pre-filter
   *                    `rhs` before doing a co-group.
   *                    Having a value close to the actual number improves the false positives
   *                    in intermediate steps which means less shuffle.
   */
  def sparseLookup[A, B](
    rhs1: SCollection[(K, A)],
    rhs2: SCollection[(K, B)],
    thisNumKeys: Long
  )(implicit funnel: Funnel[K]): SCollection[(K, (V, Iterable[A], Iterable[B]))] =
    sparseLookup(rhs1, rhs2, thisNumKeys, 0.01)

  // =======================================================================
  // Transformations
  // =======================================================================

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, `U`, than the type of the values in this
   * SCollection, `V`. Thus, we need one operation for merging a `V` into a `U` and one operation
   * for merging two `U``'s. To avoid memory allocation, both of these functions are allowed to
   * modify and return their first argument instead of creating a new `U`.
   * @group per_key
   */
  def aggregateByKey[U: Coder](
    zeroValue: => U
  )(seqOp: (U, V) => U, combOp: (U, U) => U): SCollection[(K, U)] =
    this
      .applyPerKey(
        Combine.perKey(Functions.aggregateFn(context, zeroValue)(seqOp, combOp))
      )(kvToTuple)

  /**
   * Aggregate the values of each key with [[com.twitter.algebird.Aggregator Aggregator]]. First
   * each value `V` is mapped to `A`, then we reduce with a
   * [[com.twitter.algebird.Semigroup Semigroup]] of `A`, then finally we present the results as
   * `U`. This could be more powerful and better optimized in some cases.
   * @group per_key
   */
  def aggregateByKey[A: Coder, U: Coder](aggregator: Aggregator[V, A, U]): SCollection[(K, U)] =
    self.transform { in =>
      val a = aggregator // defeat closure
      in.mapValues(a.prepare)
        .sumByKey(a.semigroup)
        .mapValues(a.present)
    }

  /**
   * Aggregate the values of each key with
   * [[com.twitter.algebird.MonoidAggregator MonoidAggregator]]. First each value `V` is mapped to
   * `A`, then we reduce with a [[com.twitter.algebird.Monoid Monoid]] of `A`, then finally we
   * present the results as `U`. This could be more powerful and better optimized in some cases.
   * @group per_key
   */
  def aggregateByKey[A: Coder, U: Coder](
    aggregator: MonoidAggregator[V, A, U]
  ): SCollection[(K, U)] = self.transform { in =>
    val a = aggregator // defeat closure
    in.mapValues(a.prepare)
      .foldByKey(a.monoid)
      .mapValues(a.present)
  }

  /**
   * For each key, compute the values' data distribution using approximate `N`-tiles.
   * @return a new SCollection whose values are `Iterable`s of the approximate `N`-tiles of
   * the elements.
   * @group per_key
   */
  def approxQuantilesByKey(
    numQuantiles: Int
  )(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(ApproximateQuantiles.perKey(numQuantiles, ord))(kvListToTuple)

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an `SCollection[(K, V)]` into a result of type `SCollection[(K, C)]`, for a
   * "combined type" `C` Note that `V` and `C` can be different -- for example, one might group an
   * SCollection of type `(Int, Int)` into an SCollection of type `(Int, Seq[Int])`. Users provide
   * three functions:
   *
   * - `createCombiner`, which turns a `V` into a `C` (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a `V` into a `C` (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two `C`'s into a single one.
   *
   * Both `mergeValue` and `mergeCombiners` are allowed to modify and return their first argument
   * instead of creating a new `U` to avoid memory allocation.
   *
   * @group per_key
   */
  def combineByKey[C: Coder](
    createCombiner: V => C
  )(mergeValue: (C, V) => C)(mergeCombiners: (C, C) => C): SCollection[(K, C)] = {
    PairSCollectionFunctions.logger.warn(
      "combineByKey/sumByKey does not support default value and may fail in some streaming " +
        "scenarios. Consider aggregateByKey/foldByKey instead."
    )
    this.applyPerKey(
      Combine.perKey(
        Functions.combineFn(context, createCombiner, mergeValue, mergeCombiners)
      )
    )(kvToTuple)
  }

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param sampleSize the number of entries in the statistical sample; the higher this number, the
   * more accurate the estimate will be; should be `>= 16`.
   * @group per_key
   */
  def countApproxDistinctByKey(sampleSize: Int): SCollection[(K, Long)] =
    ApproximateUniqueCounter(sampleSize)
      .estimateDistinctCountPerKey(this.self)

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param maximumEstimationError the maximum estimation error, which should be in the range
   * `[0.01, 0.5]`.
   * @group per_key
   */
  def countApproxDistinctByKey(maximumEstimationError: Double = 0.02): SCollection[(K, Long)] =
    ApproximateUniqueCounterByError(maximumEstimationError)
      .estimateDistinctCountPerKey(this.self)

  /**
   * Return a new SCollection of (key, value) pairs where value is estimated distinct count(as Long) per each unique key.
   * Correctness of the estimation is depends on the given [[ApproxDistinctCounter]] estimator.
   *
   * @example
   * {{{
   *   val input: SCollection[(K, V)] = ...
   *   val distinctCount: SCollection[(K, Long)] =
   *       input.approximateDistinctCountPerKey(ApproximateUniqueCounter(sampleSize))
   * }}}
   *
   * There are two known subclass of [[ApproxDistinctCounter]] available for HLL++ implementations in
   * the `scio-extra` module.
   *
   *  - [[com.spotify.scio.extra.hll.sketching.SketchingHyperLogLogPlusPlus]]
   *  - [[com.spotify.scio.extra.hll.zetasketch.ZetasketchHll_Counter]]
   *
   * HyperLogLog++: [[https://research.google/pubs/pub40671/ Google HLL++ paper]]
   *
   * @return a key valued SCollection where value type is Long and hold the approximate distinct count.
   */
  def countApproxDistinctByKey(estimator: ApproxDistinctCounter[V]): SCollection[(K, Long)] =
    estimator.estimateDistinctCountPerKey(this.self)

  /**
   * Count the number of elements for each key.
   * @return a new SCollection of (key, count) pairs
   * @group per_key
   */
  def countByKey: SCollection[(K, Long)] =
    self.transform(_.keys.countByValue)

  /**
   * Return a new SCollection of (key, value) pairs without duplicates based on the keys.
   * The value is taken randomly for each key.
   *
   * @return a new SCollection of (key, value) pairs
   * @group per_key
   */
  def distinctByKey: SCollection[(K, V)] =
    self.distinctBy(_._1)

  /**
   * Return a new SCollection of (key, value) pairs whose values satisfy the predicate.
   * @group transform
   */
  def filterValues(f: V => Boolean): SCollection[(K, V)] =
    self.filter(kv => f(kv._2))

  /**
   * Pass each value in the key-value pair SCollection through a `flatMap` function without
   * changing the keys.
   * @group transform
   */
  def flatMapValues[U: Coder](f: V => TraversableOnce[U]): SCollection[(K, U)] =
    self.flatMap(kv => f(kv._2).map(v => (kv._1, v)))

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.). The
   * function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   *
   * @group per_key
   */
  def foldByKey(zeroValue: => V)(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.aggregateFn(context, zeroValue)(op, op)))(
      kvToTuple
    )

  /**
   * Fold by key with [[com.twitter.algebird.Monoid Monoid]], which defines the associative
   * function and "zero value" for `V`. This could be more powerful and better optimized in some
   * cases.
   * @group per_key
   */
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(context, mon)))(kvToTuple)

  /**
   * Group the values for each key in the SCollection into a single sequence. The ordering of
   * elements within each group is not guaranteed, and may even differ each time the resulting
   * SCollection is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using
   * [[PairSCollectionFunctions.aggregateByKey[U]* PairSCollectionFunctions.aggregateByKey]] or
   * [[PairSCollectionFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, `groupByKey` must be able to hold all the key-value pairs for
   * any key in memory. If a key has too many values, it can result in an `OutOfMemoryError`.
   * @group per_key
   */
  def groupByKey: SCollection[(K, Iterable[V])] =
    this
      .applyPerKey(GroupByKey.create[K, V]())(kvIterableToTuple)
      .withState(_.copy(postGbkOp = true))

  /**
   * Batches inputs to a desired batch size. Batches will contain only elements of a single key.
   *
   * Elements are buffered until there are batchSize elements buffered, at which point they are
   * outputed to the output [[SCollection]].
   *
   * Windows are preserved (batches contain elements from the same window).
   * Batches may contain elements from more than one bundle.
   *
   * @param batchSize
   *
   * @group per_key
   */
  def batchByKey(batchSize: Long): SCollection[(K, Iterable[V])] =
    this.applyPerKey(GroupIntoBatches.ofSize(batchSize))(kvIterableToTuple)

  /**
   * Return an SCollection with the pairs from `this` whose keys are in `rhs`.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per_key
   */
  def intersectByKey(rhs: SCollection[K]): SCollection[(K, V)] = self.transform {
    _.cogroup(rhs.map((_, ()))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.nonEmpty) t._2._1.map((t._1, _))
      else Seq.empty
    }
  }

  /**
   * Return an SCollection with the pairs from `this` whose keys might be present
   * in the [[SideInput]].
   *
   * The `SideInput[ApproxFilter]` can be used reused for multiple sparse operations
   * across multiple SCollections.
   *
   * @example
   * {{{
   *   val si = pairSCollRight.asApproxFilterSideInput(BloomFilter, 1000000)
   *   val filtered1 = pairSColl1.sparseIntersectByKey(si)
   *   val filtered2 = pairSColl2.sparseIntersectByKey(si)
   * }}}
   * @group per_key
   */
  def sparseIntersectByKey[AF <: ApproxFilter[K]](sideInput: SideInput[AF]): SCollection[(K, V)] =
    self.transform {
      _.withSideInputs(sideInput).filter { case ((k, _), c) =>
        c(sideInput).mightContain(k)
      }.toSCollection
    }

  /**
   * Return an SCollection with the pairs from `this` whose keys are in `rhs`
   * when the cardinality of `this` >> `rhs`, but neither can fit in memory
   * (see [[PairHashSCollectionFunctions.hashIntersectByKey]]).
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @param rhsNumKeys  An estimate of the number of keys in `rhs`. This estimate is used to find
   *                     the size and number of BloomFilters that Scio would use to pre-filter
   *                     `this` in a "map" step before any join.
   *                     Having a value close to the actual number improves the false positives
   *                     in output. When `computeExact` is set to true, a more accurate estimate
   *                     of the number of keys in `rhs` would mean less shuffle when finding the
   *                     exact value.
   *
   * @param computeExact Whether or not to directly pass through bloom filter results (with a small
   *                     false positive rate) or perform an additional inner join to confirm
   *                     exact result set. By default this is set to false.
   *
   * @param fpProb       A fraction in range (0, 1) which would be the accepted false positive
   *                     probability for this transform. By default when `computeExact` is set to
   *                     `false`, this reflects the probability that an output element is an
   *                     incorrect intersect (meaning it may not be present in `rhs`)
   *                     When `computeExact` is set to `true`, this fraction is used to find the
   *                     acceptable false positive in the intermediate step before computing exact.
   *                     Note: having fpProb = 0 doesn't mean an exact computation. This value
   *                     along with `rhsNumKeys` is used for creating a BloomFilter.
   *
   * @group per_key
   */
  def sparseIntersectByKey(
    rhs: SCollection[K],
    rhsNumKeys: Long,
    computeExact: Boolean = false,
    fpProb: Double = 0.01
  )(implicit funnel: Funnel[K]): SCollection[(K, V)] =
    self.transform { me =>
      val rhsBfs = BloomFilter.createPartitionedSideInputs(rhs, rhsNumKeys, fpProb)
      val n = rhsBfs.size
      val thisParts = me.hashPartitionByKey(n)
      val rhsParts = rhs.hashPartition(n)
      SCollection.unionAll(
        thisParts
          .zip(rhsParts)
          .zip(rhsBfs)
          .map { case ((lhs, rhs), rhsBf) =>
            val approxResults = lhs
              .withSideInputs(rhsBf)
              .filter { case (e, c) => c(rhsBf).mightContain(e._1) }
              .toSCollection

            if (computeExact) {
              approxResults
                .intersectByKey(rhs)
            } else {
              approxResults
            }
          }
      )
    }

  /**
   * Return an SCollection with the keys of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Keys
  def keys: SCollection[K] =
    self.map(_._1)

  /**
   * Pass each key in the key-value pair SCollection through a `map` function without changing
   * the values.
   * @group transform
   */
  def mapKeys[U: Coder](f: K => U): SCollection[(U, V)] =
    self.map(kv => (f(kv._1), kv._2))

  /**
   * Pass each value in the key-value pair SCollection through a `map` function without changing
   * the keys.
   * @group transform
   */
  def mapValues[U: Coder](f: V => U): SCollection[(K, U)] =
    self.map(kv => (kv._1, f(kv._2)))

  /**
   * Return the max of values for each key as defined by the implicit `Ordering[T]`.
   * @return a new SCollection of (key, maximum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Max
  def maxByKey(implicit ord: Ordering[V]): SCollection[(K, V)] =
    this.reduceByKey(ord.max)

  /**
   * Return the min of values for each key as defined by the implicit `Ordering[T]`.
   * @return a new SCollection of (key, minimum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Min
  def minByKey(implicit ord: Ordering[V]): SCollection[(K, V)] =
    this.reduceByKey(ord.min)

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   * @group per_key
   */
  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(context, op)))(kvToTuple)

  /**
   * Return a sampled subset of values for each key of this SCollection.
   * @return a new SCollection of (key, sampled values) pairs
   * @group per_key
   */
  def sampleByKey(sampleSize: Int): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Sample.fixedSizePerKey[K, V](sampleSize))(kvIterableToTuple)

  /**
   * Return a subset of this SCollection sampled by key (via stratified sampling).
   *
   * Create a sample of this SCollection using variable sampling rates for different keys as
   * specified by `fractions`, a key to sampling rate map, via simple random sampling with one
   * pass over the SCollection, to produce a sample of size that's approximately equal to the sum
   * of `math.ceil(numItems * samplingRate)` over all key values.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @return SCollection containing the sampled subset
   * @group per_key
   */
  def sampleByKey(withReplacement: Boolean, fractions: Map[K, Double]): SCollection[(K, V)] =
    if (withReplacement) {
      self.parDo(new PoissonValueSampler[K, V](fractions))
    } else {
      self.parDo(new BernoulliValueSampler[K, V](fractions))
    }

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in `rhs`.
   * @group per_key
   */
  def subtractByKey(rhs: SCollection[K]): SCollection[(K, V)] = self.transform {
    _.cogroup(rhs.map((_, ()))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.isEmpty) t._2._1.map((t._1, _))
      else Seq.empty
    }
  }

  /**
   * Reduce by key with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful
   * and better optimized than [[reduceByKey]] in some cases.
   * @group per_key
   */
  def sumByKey(implicit sg: Semigroup[V]): SCollection[(K, V)] = {
    PairSCollectionFunctions.logger.warn(
      "combineByKey/sumByKey does not support default value and may fail in some streaming " +
        "scenarios. Consider aggregateByKey/foldByKey instead."
    )
    this.applyPerKey(Combine.perKey(Functions.reduceFn(context, sg)))(kvToTuple)
  }

  /**
   * Swap the keys with the values.
   * @group transform
   */
  // Scala lambda is simpler than transforms.KvSwap
  def swap: SCollection[(V, K)] =
    self.map(kv => (kv._2, kv._1))

  /**
   * Return the top `num` (largest) values for each key from this SCollection as defined by the
   * specified implicit `Ordering[T]`.
   *
   * @return a new SCollection of (key, top `num` values) pairs
   * @group per_key
   */
  def topByKey(num: Int)(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Top.perKey[K, V, Ordering[V]](num, ord))(kvListToTuple)

  /**
   * Return an SCollection with the values of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Values
  def values: SCollection[V] = self.map(_._2)

  /**
   * Return an SCollection having its values flattened.
   * @group transform
   */
  def flattenValues[U: Coder](implicit ev: V <:< TraversableOnce[U]): SCollection[(K, U)] =
    self.flatMapValues(_.asInstanceOf[TraversableOnce[U]])

  // =======================================================================
  // Side input operations
  // =======================================================================

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
   * `Map[key, value]`, to be used with [[SCollection.withSideInputs]]. It is required that each
   * key of the input be associated with a single value.
   *
   * Note: the underlying map implementation is runner specific and may have performance overhead.
   * Use [[asMapSingletonSideInput]] instead if the resulting map can fit into memory.
   */
  def asMapSideInput: SideInput[Map[K, V]] =
    new MapSideInput[K, V](self.transform_(_.toKV.applyInternal(View.asMap())))

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
   * `Map[key, Iterable[value]]`, to be used with [[SCollection.withSideInputs]]. In contrast to
   * [[asMapSideInput]], it is not required that the keys in the input collection be unique.
   *
   * Note: the underlying map implementation is runner specific and may have performance overhead.
   * Use [[asMultiMapSingletonSideInput]] instead if the resulting map can fit into memory.
   */
  def asMultiMapSideInput: SideInput[Map[K, Iterable[V]]] =
    new MultiMapSideInput[K, V](self.transform_(_.toKV.applyInternal(View.asMultimap())))

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
   * `Map[key, value]`, to be used with [[SCollection.withSideInputs]]. It is required that each
   * key of the input be associated with a single value.
   *
   * Currently, the resulting map is required to fit into memory. This is preferable to
   * [[asMapSideInput]] if that's the case.
   */
  def asMapSingletonSideInput: SideInput[Map[K, V]] =
    self
      .transform(
        _.groupByKey
          .map { kv =>
            require(kv._2.size == 1, s"Multiple values for key ${kv._1}")
            (kv._1, kv._2.head)
          }
          .groupBy(_ => ())
          .map[Map[K, V]](_._2.toMap)
      )
      .asSingletonSideInput(Map.empty[K, V])

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
   * `Map[key, Iterable[value]]`, to be used with [[SCollection.withSideInputs]]. In contrast to
   * [[asMapSingletonSideInput]], it is not required that the keys in the input collection be
   * unique.
   *
   * Currently, the resulting map is required to fit into memory. This is preferable to
   * [[asMultiMapSideInput]] if that's the case.
   */
  def asMultiMapSingletonSideInput: SideInput[Map[K, Iterable[V]]] =
    self
      .transform(
        _.groupByKey
          .groupBy(_ => ())
          .map[Map[K, Iterable[V]]](_._2.toMap)
      )
      .asSingletonSideInput(Map.empty[K, Iterable[V]])

  /** Returns an [[SCollection]] consisting of a single `Map[K, V]` element. */
  def reifyAsMapInGlobalWindow: SCollection[Map[K, V]] =
    self.reifyInGlobalWindow(_.asMapSideInput)

  /** Returns an [[SCollection]] consisting of a single `Map[K, Iterable[V]]` element. */
  def reifyAsMultiMapInGlobalWindow: SCollection[Map[K, Iterable[V]]] =
    self.reifyInGlobalWindow(_.asMultiMapSideInput)
}
