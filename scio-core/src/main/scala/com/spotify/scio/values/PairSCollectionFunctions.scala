/*
 * Copyright 2016 Spotify AB.
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

import java.lang.{Iterable => JIterable, Long => JLong}
import java.util.{Map => JMap}

import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.{KV, PCollection, PCollectionView}
import com.spotify.scio.ScioContext
import com.spotify.scio.util._
import com.spotify.scio.util.random.{BernoulliValueSampler, PoissonValueSampler}
import com.twitter.algebird.{Aggregator, _}

import scala.reflect.ClassTag

// scalastyle:off number.of.methods
/**
 * Extra functions available on SCollections of (key, value) pairs through an implicit conversion.
 *
 * @groupname cogroup CoGroup Operations
 * @groupname join Join Operations
 * @groupname per_key Per Key Aggregations
 * @groupname transform Transformations
 * @groupname Ungrouped Other Members
 */
class PairSCollectionFunctions[K, V](val self: SCollection[(K, V)])
                                    (implicit ctKey: ClassTag[K], ctValue: ClassTag[V]) {

  import TupleFunctions._

  private val context: ScioContext = self.context

  private def toKvTransform = ParDo.of(Functions.mapFn[(K, V), KV[K, V]](kv => KV.of(kv._1, kv._2)))

  private[scio] def toKV: SCollection[KV[K, V]] = {
    val o = self.applyInternal(toKvTransform).setCoder(self.getKvCoder[K, V])
    context.wrap(o)
  }

  private[values] def applyPerKey[UI: ClassTag, UO: ClassTag]
  (t: PTransform[PCollection[KV[K, V]], PCollection[KV[K, UI]]], f: KV[K, UI] => (K, UO))
  : SCollection[(K, UO)] = {
    val o = self.applyInternal(new PTransform[PCollection[(K, V)], PCollection[(K, UO)]]() {
      override def apply(input: PCollection[(K, V)]): PCollection[(K, UO)] =
        input
          .apply("TupleToKv", toKvTransform)
          .setCoder(self.getKvCoder[K, V])
          .apply(t)
          .apply("KvToTuple", ParDo.of(Functions.mapFn[KV[K, UI], (K, UO)](f)))
          .setCoder(self.getCoder[(K, UO)])
    })
    context.wrap(o)
  }

  /**
   * Convert this SCollection to an [[SCollectionWithHotKeyFanout]] that uses an intermediate node
   * to combine "hot" keys partially before performing the full combine.
   * @param hotKeyFanout a function from keys to an integer N, where the key will be spread among
   * N intermediate nodes for partial combining. If N is less than or equal to 1, this key will
   * not be sent through an intermediate node.
   */
  def withHotKeyFanout(hotKeyFanout: K => Int): SCollectionWithHotKeyFanout[K, V] =
    new SCollectionWithHotKeyFanout(this, Left(hotKeyFanout))

  /**
   * Convert this SCollection to an [[SCollectionWithHotKeyFanout]] that uses an intermediate node
   * to combine "hot" keys partially before performing the full combine.
   * @param hotKeyFanout constant value for every key
   */
  def withHotKeyFanout(hotKeyFanout: Int): SCollectionWithHotKeyFanout[K, V] =
    new SCollectionWithHotKeyFanout(this, Right(hotKeyFanout))

  // =======================================================================
  // CoGroups
  // =======================================================================

  /**
   * For each key k in `this` or `that`, return a resulting SCollection that contains a tuple with
   * the list of values for that key in `this` as well as `that`.
   * @group cogroup
   */
  def cogroup[W: ClassTag](that: SCollection[(K, W)])
  : SCollection[(K, (Iterable[V], Iterable[W]))] =
    MultiJoin.cogroup(self, that)

  /**
   * For each key k in `this` or `that1` or `that2`, return a resulting SCollection that contains
   * a tuple with the list of values for that key in `this`, `that1` and `that2`.
   * @group cogroup
   */
  def cogroup[W1: ClassTag, W2: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    MultiJoin.cogroup(self, that1, that2)

  /**
   * For each key k in `this` or `that1` or `that2` or `that3`, return a resulting SCollection
   * that contains a tuple with the list of values for that key in `this`, `that1`, `that2` and
   * `that3`.
   * @group cogroup
   */
  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    MultiJoin.cogroup(self, that1, that2, that3)

  /**
   * Alias for cogroup.
   * @group cogroup
   */
  def groupWith[W: ClassTag](that: SCollection[(K, W)])
  : SCollection[(K, (Iterable[V], Iterable[W]))] =
    this.cogroup(that)

  /**
   * Alias for cogroup.
   * @group cogroup
   */
  def groupWith[W1: ClassTag, W2: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    this.cogroup(that1, that2)

  /**
   * Alias for cogroup.
   * @group cogroup
   */
  def groupWith[W1: ClassTag, W2: ClassTag, W3: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    this.cogroup(that1, that2, that3)

  // =======================================================================
  // Joins
  // =======================================================================

  /**
   * Perform a full outer join of `this` and `that`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), Some(w))) for w in `that`,
   * or the pair (k, (Some(v), None)) if no elements in `that` have key k. Similarly, for each
   * element (k, w) in `that`, the resulting SCollection will either contain all pairs (k,
   * (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements in
   * `this` have key k. Uses the given Partitioner to partition the output SCollection.
   * @group join
   */
  def fullOuterJoin[W: ClassTag](that: SCollection[(K, W)])
  : SCollection[(K, (Option[V], Option[W]))] =
    MultiJoin.outer(self, that)

  /**
   * Return an SCollection containing all pairs of elements with matching keys in `this` and
   * `that`. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in
   * `this` and (k, v2) is in `that`. Uses the given Partitioner to partition the output RDD.
   * @group join
   */
  def join[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, W))] =
    MultiJoin(self, that)

  /**
   * Perform a left outer join of `this` and `that`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (v, Some(w))) for w in `that`, or the
   * pair (k, (v, None)) if no elements in `that` have key k. Uses the given Partitioner to
   * partition the output SCollection.
   * @group join
   */
  def leftOuterJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, Option[W]))] =
    MultiJoin.left(self, that)

  /**
   * Perform a right outer join of `this` and `that`. For each element (k, w) in `that`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output SCollection.
   * @group join
   */
  def rightOuterJoin[W: ClassTag](that: SCollection[(K, W)])
  : SCollection[(K, (Option[V], W))] = self.transform {
    MultiJoin.left(that, _).mapValues(kv => (kv._2, kv._1))
  }

  /* Hash operations */

  /**
   * Perform an inner join by replicating `that` to all workers. The right side should be tiny and
   * fit in memory.
   *
   * @group join
   */
  def hashJoin[W: ClassTag](that: SCollection[(K, W)])
  : SCollection[(K, (V, W))] = self.transform { in =>
    val side = that.asMultiMapSideInput
    in.withSideInputs(side).flatMap[(K, (V, W))] { (kv, s) =>
      s(side).getOrElse(kv._1, Iterable()).toSeq.map(w => (kv._1, (kv._2, w)))
    }.toSCollection
  }

  /**
   * Perform a left outer join by replicating `that` to all workers. The right side should be tiny
   * and fit in memory.
   *
   * @group join
   */
  def hashLeftJoin[W: ClassTag](that: SCollection[(K, W)])
  : SCollection[(K, (V, Option[W]))] = self.transform { in =>
    val side = that.asMultiMapSideInput
    in.withSideInputs(side).flatMap[(K, (V, Option[W]))] { (kv, s) =>
      val (k, v) = kv
      val m = s(side)
      if (m.contains(k)) m(k).map(w => (k, (v, Some(w)))) else Seq((k, (v, None)))
    }.toSCollection
  }

  /**
   * N to 1 skewproof flavor of [[PairSCollectionFunctions.join()]].
   *
   * Perform a skewed join where some keys on the left hand may be hot, i.e. appear more than
   * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
   * estimate is within `eps * N` of the true frequency.
   * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
   * the left hand side stream so far.
   *
   * @note Make sure to import [[com.twitter.algebird.CMSHasherImplicits]] before using this join
   * @example {{{
   * // Implicits that enabling CMS-hashing
   * import com.twitter.algebird.CMSHasherImplicits._
   *
   * val p = logs.skewedJoin(logMetadata, hotKeyThreshold = 8500, eps=0.0005, seed=1)
   * }}}
   *
   * Read more about CMS -> [[com.twitter.algebird.CMSMonoid]]
   * @group join
   * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
   *                        have inefficient GroupByKey implementation for groups with more than 10K
   *                        values. Thus it is recommended to set `hotKeyThreshold` to below 10K,
   *                        keep upper estimation error in mind.
   * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
   *            Must lie in (0, 1).
   * @param seed A seed to initialize the random number generator used to create the pairwise
   *             independent hash functions.
   * @param delta A bound on the probability that a query estimate does not lie within some small
   *              interval (an interval that depends on `eps`) around the truth. Must lie in (0, 1).
   * @param sampleFraction left side sample fracation. Default is `1.0` - no sampling.
   * @param withReplacement whether to use sampling with replacement, see [[SCollection.sample()]]
   */
  def skewedJoin[W: ClassTag](that: SCollection[(K, W)],
                              hotKeyThreshold: Long,
                              eps: Double,
                              seed: Int,
                              delta: Double = 1E-10,
                              sampleFraction: Double = 1.0,
                              withReplacement: Boolean = true)(implicit hasher: CMSHasher[K])
  : SCollection[(K, (V, W))] = {
    require(sampleFraction <= 1.0 && sampleFraction > 0.0,
      "Sample fraction has to be between (0.0, 1.0] - default is 1.0")

    import com.twitter.algebird._
    // Key aggregator for `k->#values`
    val keyAggregator = CMS.aggregator[K](eps, delta, seed)

    val leftSideKeys = if (sampleFraction < 1.0) {
      self.sample(withReplacement, sampleFraction).keys
    } else {
      self.keys
    }

    val cms = leftSideKeys.aggregate(keyAggregator)
    self.skewedJoin(that, hotKeyThreshold, cms)
  }

  /**
   * N to 1 skewproof flavor of [[PairSCollectionFunctions.join()]].
   *
   * Perform a skewed join where some keys on the left hand may be hot, i.e. appear more than
   * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
   * estimate is within `eps * N` of the true frequency.
   * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
   * the left hand side stream so far.
   *
   * @note Make sure to import [[com.twitter.algebird.CMSHasherImplicits]] before using this join
   * @example {{{
   * // Implicits that enabling CMS-hashing
   * import com.twitter.algebird.CMSHasherImplicits._
   *
   * val keyAggregator = CMS.aggregator[K](eps, delta, seed)
   * val hotKeyCMS = self.keys.aggregate(keyAggregator)
   * val p = logs.skewedJoin(logMetadata, hotKeyThreshold = 8500, cms=hotKeyCMS)
   * }}}
   *
   * Read more about CMS -> [[com.twitter.algebird.CMSMonoid]]
   * @group join
   * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
   *                        have inefficient GroupByKey implementation for groups with more than 10K
   *                        values. Thus it is recommended to set `hotKeyThreshold` to below 10K,
   *                        keep upper estimation error in mind.
   * @param cms left hand side key [[com.twitter.algebird.CMSMonoid]]
   */
  def skewedJoin[W: ClassTag](that: SCollection[(K, W)],
                              hotKeyThreshold: Long,
                              cms: SCollection[CMS[K]])
  : SCollection[(K, (V, W))] = {
    val (hotSelf, chillSelf) = (SideOutput[(K, V)](), SideOutput[(K, V)]())
    // scalastyle:off line.size.limit
    // Use asIterableSideInput as workaround for:
    // http://stackoverflow.com/questions/37126729/ismsinkwriter-expects-keys-to-be-written-in-strictly-increasing-order
    // scalastyle:on line.size.limit
    val keyCMS = cms.asIterableSideInput

    val partitionedSelf = self
      .withSideInputs(keyCMS).transformWithSideOutputs(Seq(hotSelf, chillSelf), (e, c) =>
        if (c(keyCMS).nonEmpty &&
            c(keyCMS).head.frequency(e._1).estimate >= hotKeyThreshold) {
          hotSelf
        } else {
          chillSelf
        }
    )

    val (hotThat, chillThat) = (SideOutput[(K, W)](), SideOutput[(K, W)]())
    val partitionedThat = that
      .withSideInputs(keyCMS)
      .transformWithSideOutputs(Seq(hotThat, chillThat), (e, c) =>
        if (c(keyCMS).nonEmpty &&
            c(keyCMS).head.frequency(e._1).estimate >= hotKeyThreshold) {
          hotThat
        } else {
          chillThat
        }
      )

    // Use hash join for hot keys
    val hotJoined = partitionedSelf(hotSelf).hashJoin(partitionedThat(hotThat))

    // Use regular join for the rest of the keys
    val chillJoined = partitionedSelf(chillSelf).join(partitionedThat(chillThat))

    hotJoined ++ chillJoined
  }

  // =======================================================================
  // Transformations
  // =======================================================================

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this
   * SCollection, V. Thus, we need one operation for merging a V into a U and one operation for
   * merging two U's. To avoid memory allocation, both of these functions are allowed to modify
   * and return their first argument instead of creating a new U.
   * @group per_key
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                combOp: (U, U) => U): SCollection[(K, U)] =
    this.applyPerKey(
      Combine.perKey(Functions.aggregateFn(zeroValue)(seqOp, combOp)),
      kvToTuple[K, U])

  /**
   * Aggregate the values of each key with [[com.twitter.algebird.Aggregator Aggregator]]. First
   * each value V is mapped to A, then we reduce with a semigroup of A, then finally we present
   * the results as U. This could be more powerful and better optimized in some cases.
   * @group per_key
   */
  def aggregateByKey[A: ClassTag, U: ClassTag](aggregator: Aggregator[V, A, U])
  : SCollection[(K, U)] = self.transform { in =>
    val a = aggregator  // defeat closure
    in.mapValues(a.prepare).sumByKey(a.semigroup).mapValues(a.present)
  }

  /**
   * For each key, compute the values' data distribution using approximate `N`-tiles.
   * @return a new SCollection whose values are Iterables of the approximate `N`-tiles of
   * the elements.
   * @group per_key
   */
  def approxQuantilesByKey(numQuantiles: Int)(implicit ord: Ordering[V])
  : SCollection[(K, Iterable[V])] =
    this.applyPerKey(
      ApproximateQuantiles.perKey(numQuantiles, ord),
      kvListToTuple[K, V])

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an SCollection[(K, V)] into a result of type SCollection[(K, C)], for a
   * "combined type" C Note that V and C can be different -- for example, one might group an
   * SCollection of type (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three
   * functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two C's into a single one.
   * @group per_key
   */
  def combineByKey[C: ClassTag](createCombiner: V => C)
                               (mergeValue: (C, V) => C)
                               (mergeCombiners: (C, C) => C): SCollection[(K, C)] =
    this.applyPerKey(
      Combine.perKey(Functions.combineFn(createCombiner, mergeValue, mergeCombiners)),
      kvToTuple[K, C])

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param sampleSize the number of entries in the statisticalsample; the higher this number, the
   * more accurate the estimate will be; should be `>= 16`.
   * @group per_key
   */
  def countApproxDistinctByKey(sampleSize: Int): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](sampleSize), kvToTuple[K, JLong])
      .asInstanceOf[SCollection[(K, Long)]]

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param maximumEstimationError the maximum estimation error, which should be in the range
   * `[0.01, 0.5]`.
   * @group per_key
   */
  def countApproxDistinctByKey(maximumEstimationError: Double = 0.02): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](maximumEstimationError), kvToTuple[K, JLong])
      .asInstanceOf[SCollection[(K, Long)]]

  /**
   * Count the number of elements for each key.
   * @return a new SCollection of (key, count) pairs
   * @group per_key
   */
  def countByKey: SCollection[(K, Long)] = self.transform(_.keys.countByValue)

  /**
   * Pass each value in the key-value pair SCollection through a flatMap function without changing
   * the keys.
   * @group transform
   */
  def flatMapValues[U: ClassTag](f: V => TraversableOnce[U]): SCollection[(K, U)] =
    self.flatMap(kv => f(kv._2).map(v => (kv._1, v)))

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   * @group per_key
   */
  def foldByKey(zeroValue: V)(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.aggregateFn(zeroValue)(op, op)), kvToTuple[K, V])

  /**
   * Fold by key with [[com.twitter.algebird.Monoid Monoid]], which defines the associative
   * function and "zero value" for V. This could be more powerful and better optimized in some
   * cases.
   * @group per_key
   */
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(mon)), kvToTuple[K, V])

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
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for
   * any key in memory. If a key has too many values, it can result in an OutOfMemoryError.
   * @group per_key
   */
  def groupByKey: SCollection[(K, Iterable[V])] =
    this.applyPerKey(GroupByKey.create[K, V](), kvIterableToTuple[K, V])

  /**
   * Return an SCollection with the pairs from `this` whose keys are in `that`.
   * @group per_key
   */
  def intersectByKey(that: SCollection[K]): SCollection[(K, V)] = self.transform {
    _.cogroup(that.map((_, ()))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.nonEmpty) t._2._1.map((t._1, _)) else Seq.empty
    }
  }

  /**
   * Return an SCollection with the keys of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Keys
  def keys: SCollection[K] = self.map(_._1)

  /**
   * Pass each value in the key-value pair SCollection through a map function without changing the
   * keys.
   * @group transform
   */
  def mapValues[U: ClassTag](f: V => U): SCollection[(K, U)] = self.map(kv => (kv._1, f(kv._2)))

  /**
   * Return the max of values for each key as defined by the implicit Ordering[T].
   * @return a new SCollection of (key, maximum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Max
  def maxByKey(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.max)

  /**
   * Return the min of values for each key as defined by the implicit Ordering[T].
   * @return a new SCollection of (key, minimum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Min
  def minByKey(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.min)

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   * @group per_key
   */
  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(op)), kvToTuple[K, V])

  /**
   * Return a sampled subset of values for each key of this SCollection.
   * @return a new SCollection of (key, sampled values) pairs
   * @group per_key
   */
  def sampleByKey(sampleSize: Int): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Sample.fixedSizePerKey[K, V](sampleSize), kvIterableToTuple[K, V])

  /**
   * Return a subset of this SCollection sampled by key (via stratified sampling).
   *
   * Create a sample of this SCollection using variable sampling rates for different keys as
   * specified by `fractions`, a key to sampling rate map, via simple random sampling with one
   * pass over the SCollection, to produce a sample of size that's approximately equal to the sum
   * of math.ceil(numItems * samplingRate) over all key values.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @return SCollection containing the sampled subset
   * @group per_key
   */
  def sampleByKey(withReplacement: Boolean, fractions: Map[K, Double]): SCollection[(K, V)] = {
    if (withReplacement) {
      self.parDo(new PoissonValueSampler[K, V](fractions))
    } else {
      self.parDo(new BernoulliValueSampler[K, V](fractions))
    }
  }

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in `that`.
   * @group per_key
   */
  def subtractByKey(that: SCollection[K]): SCollection[(K, V)] = self.transform {
    _.cogroup(that.map((_, ()))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.isEmpty) t._2._1.map((t._1, _)) else Seq.empty
    }
  }

  /**
   * Reduce by key with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful
   * and better optimized in some cases.
   * @group per_key
   */
  def sumByKey(implicit sg: Semigroup[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(sg)), kvToTuple[K, V])

  /**
   * Swap the keys with the values.
   * @group transform
   */
  // Scala lambda is simpler than transforms.KvSwap
  def swap: SCollection[(V, K)] = self.map(kv => (kv._2, kv._1))

  /**
   * Return the top k (largest) values for each key from this SCollection as defined by the
   * specified implicit Ordering[T].
   * @return a new SCollection of (key, top k) pairs
   * @group per_key
   */
  def topByKey(num: Int)(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Top.perKey[K, V, Ordering[V]](num, ord), kvListToTuple[K, V])

  /**
   * Return an SCollection with the values of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Values
  def values: SCollection[V] = self.map(_._2)

  // =======================================================================
  // Side input operations
  // =======================================================================

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a Map[key,
   * value], to be used with [[SCollection.withSideInputs]]. It is required that each key of the
   * input be associated with a single value.
   */
  def asMapSideInput: SideInput[Map[K, V]] = {
    val o = self.applyInternal(
      new PTransform[PCollection[(K, V)], PCollectionView[JMap[K, V]]]() {
        override def apply(input: PCollection[(K, V)]): PCollectionView[JMap[K, V]] = {
          input.apply(toKvTransform).setCoder(self.getKvCoder[K, V]).apply(View.asMap())
        }
      })
    new MapSideInput[K, V](o)
  }

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a Map[key,
   * Iterable[value]], to be used with [[SCollection.withSideInputs]]. It is not required that the
   * keys in the input collection be unique.
   */
  def asMultiMapSideInput: SideInput[Map[K, Iterable[V]]] = {
    val o = self.applyInternal(
      new PTransform[PCollection[(K, V)], PCollectionView[JMap[K, JIterable[V]]]]() {
        override def apply(input: PCollection[(K, V)]): PCollectionView[JMap[K, JIterable[V]]] = {
          input.apply(toKvTransform).setCoder(self.getKvCoder[K, V]).apply(View.asMultimap())
        }
      })
    new MultiMapSideInput[K, V](o)
  }

}
// scalastyle:on number.of.methods
