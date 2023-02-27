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

import com.spotify.scio.coders.Coder
import com.twitter.algebird.{CMS, CMSAggregator, CMSHasher, TopCMS, TopCMSAggregator}

final private case class Partitions[K, V](hot: SCollection[(K, V)], chill: SCollection[(K, V)])

/** Method to compute the hot keys in a SCollection */
sealed trait HotKeyMethod
object HotKeyMethod {

  /**
   * keys with more appearances than the threshold value will be considered hot. Some runners have
   * inefficient `GroupByKey` implementation for groups with more than 10K values. Thus it is
   * recommended to set the threshold value to below 10K, keep upper estimation error in mind. If
   * you sample input via `sampleFraction` make sure to adjust threshold value accordingly.
   */
  final case class Threshold(value: Long) extends HotKeyMethod

  /**
   * keys that appear more that the percentage * total will be considered hot. This also means that
   * this parameter is an upper bound on the number of hot keys that will be tracked: the set of
   * heavy hitters contains at most 1 / percentage elements. For example, if percentage=0.01, then
   * at most 1 / 0.01 = 100 items (or if percentage=0.25, then at most 1 / 0.25 = 4 items) will be
   * tracked/returned as hot keys. This parameter can thus control the memory footprint required for
   * tracking top keys.
   */
  final case class TopPercentage(value: Double) extends HotKeyMethod

  /**
   * top N keys that appear most often are considered hot. **Warning**: The effect is that a top-N
   * CMS has an ordering bias (with regard to hot keys) when merging instances. This means merging
   * hot keys across CMS instances may lead to incorrect, biased results: the outcome is biased by
   * the order in which CMS instances / hot keys are being merged, with the rule of thumb being that
   * the earlier a set of hot keys is being merged, the more likely is the end result biased towards
   * these hot keys.
   */
  final case class TopN(value: Int) extends HotKeyMethod

}

/**
 * Extra functions available on SCollections of (key, value) pairs for skwed joins through an
 * implicit conversion.
 *
 * @groupname cogroup
 * CoGroup Operations
 * @groupname join
 * Join Operations
 * @groupname per_key
 * Per Key Aggregations
 * @groupname transform
 * Transformations
 */
class PairSkewedSCollectionFunctions[K, V](val self: SCollection[(K, V)]) {

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.join]].
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val p = logs.skewedJoin(logMetadata)
   *   }}}
   *
   * Read more about CMS: [[com.twitter.algebird.CMS]].
   * @group join
   * @param hotKeyMethod
   *   Method used to compute hot-keys from the left side collection. Default is 9000 occurrence
   *   threshold.
   * @param eps
   *   One-sided error bound on the error of each point query, i.e. frequency estimate. Must lie in
   *   `(0, 1)`.
   * @param seed
   *   A seed to initialize the random number generator used to create the pairwise independent hash
   *   functions.
   * @param delta
   *   A bound on the probability that a query estimate does not lie within some small interval (an
   *   interval that depends on `eps`) around the truth. Must lie in `(0, 1)`.
   * @param sampleFraction
   *   left side sample fraction.
   * @param withReplacement
   *   whether to use sampling with replacement, see
   *   [[SCollection.sample(withReplacement:Boolean,fraction:Double)* SCollection.sample]].
   */
  def skewedJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyMethod: HotKeyMethod,
    eps: Double,
    seed: Int,
    delta: Double,
    sampleFraction: Double,
    withReplacement: Boolean
  )(implicit hasher: CMSHasher[K]): SCollection[(K, (V, W))] = self.transform { lhs =>
    val lhsKeys = LargeLeftSide.sampleKeys(lhs, sampleFraction, withReplacement)
    import com.twitter.algebird._
    hotKeyMethod match {
      case HotKeyMethod.Threshold(value) =>
        val cms = CMSOperations.aggregate(lhsKeys, CMS.aggregator(eps, delta, seed))
        lhs.skewedJoin(rhs, value, cms)
      case HotKeyMethod.TopPercentage(value) =>
        val cms = CMSOperations.aggregate(lhsKeys, TopPctCMS.aggregator(eps, delta, seed, value))
        lhs.skewedJoin(rhs, cms)
      case HotKeyMethod.TopN(value) =>
        val cms = CMSOperations.aggregate(lhsKeys, TopNCMS.aggregator(eps, delta, seed, value))
        lhs.skewedJoin(rhs, cms)
    }
  }

  @deprecated("Use skewedJoin with HotKeyMethod.Threshold instead ", "0.12.6")
  def skewedJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyThreshold: Long = 9000,
    eps: Double = 0.001,
    seed: Int = 42,
    delta: Double = 1e-10,
    sampleFraction: Double = 1.0,
    withReplacement: Boolean = true
  )(implicit hasher: CMSHasher[K]): SCollection[(K, (V, W))] =
    skewedJoin(
      rhs,
      HotKeyMethod.Threshold(hotKeyThreshold),
      eps,
      seed,
      delta,
      sampleFraction,
      withReplacement
    )

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.join]].
   *
   * Perform a skewed join where some keys on the left hand may be hot, i.e. appear more than
   * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
   * estimate is within `eps * N` of the true frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val keyAggregator = CMS.aggregator[K](eps, delta, seed)
   *   val hotKeyCMS = self.keys.aggregate(keyAggregator)
   *   val p = logs.skewedJoin(logMetadata, hotKeyThreshold=8500, cms=hotKeyCMS)
   *   }}}
   *
   * Read more about CMS: [[com.twitter.algebird.CMS]].
   * @group join
   * @param hotKeyThreshold
   *   key with `hotKeyThreshold` values will be considered hot. Some runners have inefficient
   *   `GroupByKey` implementation for groups with more than 10K values. Thus it is recommended to
   *   set `hotKeyThreshold` to below 10K, keep upper estimation error in mind.
   * @param cms
   *   left hand side key [[com.twitter.algebird.CMS]]
   */
  def skewedJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyThreshold: Long,
    cms: SCollection[CMS[K]]
  ): SCollection[(K, (V, W))] = self.transform { lhs =>
    val (lhsPartitions, rhsPartitions) = CMSOperations.partition(lhs, rhs, cms, hotKeyThreshold)
    SkewedJoins.join(lhsPartitions, rhsPartitions)
  }

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.join]].
   *
   * Perform a skewed join where some keys on the left hand may be hot. Frequency of a key is
   * estimated with `1 - delta` probability, and the estimate is within `eps * N` of the true
   * frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val keyAggregator = TopNCMS.aggregator[K](eps, delta, seed, count)
   *   val hotKeyCMS = self.keys.aggregate(keyAggregator)
   *   val p = logs.skewedJoin(logMetadata, hotKeyCMS)
   *   }}}
   *
   * Read more about TopCMS: [[com.twitter.algebird.TopCMS]].
   * @group join
   * @param cms
   *   left hand side key [[com.twitter.algebird.TopCMS]]
   */
  def skewedJoin[W](
    rhs: SCollection[(K, W)],
    cms: SCollection[TopCMS[K]]
  ): SCollection[(K, (V, W))] = self.transform { me =>
    val (lhsPartitions, rhsPartitions) =
      CMSOperations.partition(me, rhs, cms)
    SkewedJoins.join(lhsPartitions, rhsPartitions)
  }

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.leftOuterJoin]].
   *
   * Perform a skewed left join where some keys on the left hand may be hot. Frequency of a key is
   * estimated with `1 - delta` probability, and the estimate is within `eps * N` of the true
   * frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val p = logs.skewedLeftJoin(logMetadata)
   *   }}}
   *
   * Read more about CMS: [[com.twitter.algebird.CMS]].
   * @group join
   * @param hotKeyMethod
   *   Method used to compute hot-keys from the left side collection. Default is 9000 occurrence
   *   threshold.
   * @param eps
   *   One-sided error bound on the error of each point query, i.e. frequency estimate. Must lie in
   *   `(0, 1)`.
   * @param seed
   *   A seed to initialize the random number generator used to create the pairwise independent hash
   *   functions.
   * @param delta
   *   A bound on the probability that a query estimate does not lie within some small interval (an
   *   interval that depends on `eps`) around the truth. Must lie in `(0, 1)`.
   * @param sampleFraction
   *   left side sample fraction.
   * @param withReplacement
   *   whether to use sampling with replacement, see
   *   [[SCollection.sample(withReplacement:Boolean,fraction:Double)* SCollection.sample]].
   */
  def skewedLeftOuterJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyMethod: HotKeyMethod,
    eps: Double,
    seed: Int,
    delta: Double,
    sampleFraction: Double,
    withReplacement: Boolean
  )(implicit hasher: CMSHasher[K]): SCollection[(K, (V, Option[W]))] = self.transform { lhs =>
    import com.twitter.algebird._
    val lhsKeys = LargeLeftSide.sampleKeys(lhs, sampleFraction, withReplacement)
    hotKeyMethod match {
      case HotKeyMethod.Threshold(value) =>
        val cms = CMSOperations.aggregate(lhsKeys, CMS.aggregator(eps, delta, seed))
        lhs.skewedLeftOuterJoin(rhs, value, cms)
      case HotKeyMethod.TopPercentage(value) =>
        val cms = CMSOperations.aggregate(lhsKeys, TopPctCMS.aggregator(eps, delta, seed, value))
        lhs.skewedLeftOuterJoin(rhs, cms)
      case HotKeyMethod.TopN(value) =>
        val cms = CMSOperations.aggregate(lhsKeys, TopNCMS.aggregator(eps, delta, seed, value))
        lhs.skewedLeftOuterJoin(rhs, cms)
    }
  }

  @deprecated("Use skewedLeftOuterJoin with HotKeyMethod.Threshold instead ", "0.12.6")
  def skewedLeftOuterJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyThreshold: Long = 9000,
    eps: Double = 0.001,
    seed: Int = 42,
    delta: Double = 1e-10,
    sampleFraction: Double = 1.0,
    withReplacement: Boolean = true
  )(implicit hasher: CMSHasher[K]): SCollection[(K, (V, Option[W]))] =
    skewedLeftOuterJoin(
      rhs,
      HotKeyMethod.Threshold(hotKeyThreshold),
      eps,
      seed,
      delta,
      sampleFraction,
      withReplacement
    )

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.leftOuterJoin]].
   *
   * Perform a skewed left join where some keys on the left hand may be hot, i.e. appear more than
   * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
   * estimate is within `eps * N` of the true frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val keyAggregator = CMS.aggregator[K](eps, delta, seed)
   *   val hotKeyCMS = self.keys.aggregate(keyAggregator)
   *   val p = logs.skewedLeftOuterJoin(logMetadata, hotKeyThreshold=8500, cms=hotKeyCMS)
   *   }}}
   *
   * Read more about CMS: [[com.twitter.algebird.CMS]].
   * @group join
   * @param hotKeyThreshold
   *   key with `hotKeyThreshold` values will be considered hot. Some runners have inefficient
   *   `GroupByKey` implementation for groups with more than 10K values. Thus it is recommended to
   *   set `hotKeyThreshold` to below 10K, keep upper estimation error in mind.
   * @param cms
   *   left hand side key [[com.twitter.algebird.CMS]]
   */
  def skewedLeftOuterJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyThreshold: Long,
    cms: SCollection[CMS[K]]
  ): SCollection[(K, (V, Option[W]))] = self.transform { lhs =>
    val (lhsPartitions, rhsPartitions) = CMSOperations.partition(lhs, rhs, cms, hotKeyThreshold)
    SkewedJoins.leftOuterJoin(lhsPartitions, rhsPartitions)
  }

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.leftOuterJoin]].
   *
   * Perform a skewed left join where some keys on the left hand may be hot. Frequency of a key is
   * estimated with `1 - delta` probability, and the estimate is within `eps * N` of the true
   * frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val keyAggregator = TopNCMS.aggregator[K](eps, delta, seed, count)
   *   val hotKeyCMS = self.keys.aggregate(keyAggregator)
   *   val p = logs.skewedLeftOuterJoin(logMetadata, hotKeyCMS)
   *   }}}
   *
   * Read more about TopCMS: [[com.twitter.algebird.TopCMS]].
   * @group join
   * @param cms
   *   left hand side key [[com.twitter.algebird.TopCMS]]
   */
  def skewedLeftOuterJoin[W](
    rhs: SCollection[(K, W)],
    cms: SCollection[TopCMS[K]]
  ): SCollection[(K, (V, Option[W]))] = self.transform { lhs =>
    val (lhsPartitions, rhsPartitions) = CMSOperations.partition(lhs, rhs, cms)
    SkewedJoins.leftOuterJoin(lhsPartitions, rhsPartitions)
  }

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.fullOuterJoin]].
   *
   * Perform a skewed full join where some keys on the left hand may be hot. Frequency of a key is
   * estimated with `1 - delta` probability, and the estimate is within `eps * N` of the true
   * frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val p = logs.skewedFullOuterJoin(logMetadata)
   *   }}}
   *   Read more about CMS: [[com.twitter.algebird.CMS]].
   * @group join
   * @param hotKeyMethod
   *   Method used to compute hot-keys from the left side collection.
   * @param eps
   *   One-sided error bound on the error of each point query, i.e. frequency estimate. Must lie in
   *   `(0, 1)`.
   * @param seed
   *   A seed to initialize the random number generator used to create the pairwise independent hash
   *   functions.
   * @param delta
   *   A bound on the probability that a query estimate does not lie within some small interval (an
   *   interval that depends on `eps`) around the truth. Must lie in `(0, 1)`.
   * @param sampleFraction
   *   left side sample fraction.
   * @param withReplacement
   *   whether to use sampling with replacement, see
   *   [[SCollection.sample(withReplacement:Boolean,fraction:Double)* SCollection.sample]].
   */
  def skewedFullOuterJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyMethod: HotKeyMethod,
    eps: Double,
    seed: Int,
    delta: Double,
    sampleFraction: Double,
    withReplacement: Boolean
  )(implicit hasher: CMSHasher[K]): SCollection[(K, (Option[V], Option[W]))] = self.transform {
    lhs =>
      import com.twitter.algebird._
      val lhsKeys = LargeLeftSide.sampleKeys(lhs, sampleFraction, withReplacement)
      hotKeyMethod match {
        case HotKeyMethod.Threshold(value) =>
          val cms = CMSOperations.aggregate(lhsKeys, CMS.aggregator[K](eps, delta, seed))
          lhs.skewedFullOuterJoin(rhs, value, cms)
        case HotKeyMethod.TopPercentage(value) =>
          val cms = CMSOperations.aggregate(lhsKeys, TopPctCMS.aggregator(eps, delta, seed, value))
          lhs.skewedFullOuterJoin(rhs, cms)
        case HotKeyMethod.TopN(value) =>
          val cms = CMSOperations.aggregate(lhsKeys, TopNCMS.aggregator(eps, delta, seed, value))
          lhs.skewedFullOuterJoin(rhs, cms)
      }
  }

  @deprecated("Use skewedFullOuterJoin with HotKeyMethod.Threshold instead ", "0.12.6")
  def skewedFullOuterJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyThreshold: Long = 9000,
    eps: Double = 0.001,
    seed: Int = 42,
    delta: Double = 1e-10,
    sampleFraction: Double = 1.0,
    withReplacement: Boolean = true
  )(implicit hasher: CMSHasher[K]): SCollection[(K, (Option[V], Option[W]))] =
    skewedFullOuterJoin(
      rhs,
      HotKeyMethod.Threshold(hotKeyThreshold),
      eps,
      seed,
      delta,
      sampleFraction,
      withReplacement
    )

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.fullOuterJoin]].
   *
   * Perform a skewed full outer join where some keys on the left hand may be hot, i.e.appear more
   * than`hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and
   * the estimate is within `eps * N` of the true frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val keyAggregator = CMS.aggregator[K](eps, delta, seed)
   *   val hotKeyCMS = self.keys.aggregate(keyAggregator)
   *   val p = logs.skewedFullOuterJoin(logMetadata, hotKeyThreshold=8500, cms=hotKeyCMS)
   *   }}}
   *
   * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
   * @group join
   * @param hotKeyThreshold
   *   key with `hotKeyThreshold` values will be considered hot. Some runners have inefficient
   *   `GroupByKey` implementation for groups with more than 10K values. Thus it is recommended to
   *   set `hotKeyThreshold` to below 10K, keep upper estimation error in mind.
   * @param cms
   *   left hand side key [[com.twitter.algebird.CMSMonoid]]
   */
  def skewedFullOuterJoin[W](
    rhs: SCollection[(K, W)],
    hotKeyThreshold: Long,
    cms: SCollection[CMS[K]]
  ): SCollection[(K, (Option[V], Option[W]))] = self.transform { lhs =>
    val (lhsPartitions, rhsPartitions) = CMSOperations.partition(lhs, rhs, cms, hotKeyThreshold)
    SkewedJoins.fullOuterJoin(lhsPartitions, rhsPartitions)
  }

  /**
   * N to 1 skew-proof flavor of [[PairSCollectionFunctions.fullOuterJoin]].
   *
   * Perform a skewed full outer join where some keys on the left hand may be hot. Frequency of a
   * key is estimated with `1 - delta` probability, and the estimate is within `eps * N` of the true
   * frequency.
   *
   * `true frequency <= estimate <= true frequency + eps * N`
   *
   * where N is the total size of the left hand side stream so far.
   *
   * @note
   *   Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example
   *   {{{
   *   // Implicits that enabling CMS-hashing
   *   import com.twitter.algebird.CMSHasherImplicits._
   *   val keyAggregator = TopNCMS.aggregator[K](eps, delta, seed, count)
   *   val hotKeyCMS = self.keys.aggregate(keyAggregator)
   *   val p = logs.skewedFullOuterJoin(logMetadata, hotKeyCMS)
   *   }}}
   *
   * Read more about TopCMS: [[com.twitter.algebird.TopCMS]].
   * @group join
   * @param cms
   *   left hand side key [[com.twitter.algebird.TopCMS]]
   */
  def skewedFullOuterJoin[W](
    rhs: SCollection[(K, W)],
    cms: SCollection[TopCMS[K]]
  ): SCollection[(K, (Option[V], Option[W]))] = self.transform { lhs =>
    val (lhsPartitions, rhsPartitions) = CMSOperations.partition(lhs, rhs, cms)
    SkewedJoins.fullOuterJoin(lhsPartitions, rhsPartitions)
  }
}

private object LargeLeftSide {
  def sampleKeys[K, V](
    coll: SCollection[(K, V)],
    fraction: Double,
    withReplacement: Boolean
  ): SCollection[K] = {
    require(
      fraction <= 1.0 && fraction > 0.0,
      "Sample fraction has to be between (0.0, 1.0] - default is 1.0"
    )

    if (fraction < 1.0) {
      coll.keys.withName("Sample LHS").sample(withReplacement, fraction)
    } else {
      coll.keys
    }
  }
}

private object CMSOperations {

  def aggregate[K](
    keys: SCollection[K],
    aggregator: CMSAggregator[K]
  ): SCollection[CMS[K]] =
    keys.withName("Compute CMS of LHS keys").aggregate(aggregator)

  def aggregate[K](
    keys: SCollection[K],
    aggregator: TopCMSAggregator[K]
  ): SCollection[TopCMS[K]] =
    keys.withName("Compute CMS of LHS keys").aggregate(aggregator)

  def partition[K, V, W](
    lhs: SCollection[(K, V)],
    rhs: SCollection[(K, W)],
    hotKeyCms: SCollection[CMS[K]],
    hotKeyThreshold: Long
  ): (Partitions[K, V], Partitions[K, W]) = {
    implicit val kCoder: Coder[K] = lhs.keyCoder
    implicit val vCoder: Coder[V] = lhs.valueCoder
    implicit val wCoder: Coder[W] = rhs.valueCoder

    val cmsSideInput = hotKeyCms.asSingletonSideInput
    val thresholdSideInput = hotKeyCms
      .withName("Compute CMS threshold with error bound")
      .map(c => hotKeyThreshold + c.totalCount * c.eps)
      .asSingletonSideInput

    val (hotLhs, chillLhs) = (SideOutput[(K, V)](), SideOutput[(K, V)]())
    val (hotRhs, chillRhs) = (SideOutput[(K, W)](), SideOutput[(K, W)]())

    val partitionedLhs = lhs
      .withSideInputs(cmsSideInput, thresholdSideInput)
      .transformWithSideOutputs(Seq(hotLhs, chillLhs), "Partition LHS") { case ((k, _), ctx) =>
        val cms = ctx(cmsSideInput)
        val threshold = ctx(thresholdSideInput)
        if (cms.frequency(k).estimate >= threshold) hotLhs else chillLhs
      }

    val partitionedRhs = rhs
      .withSideInputs(cmsSideInput, thresholdSideInput)
      .transformWithSideOutputs(Seq(hotRhs, chillRhs), "Partition RHS") { case ((k, _), ctx) =>
        val cms = ctx(cmsSideInput)
        val threshold = ctx(thresholdSideInput)
        if (cms.frequency(k).estimate >= threshold) hotRhs else chillRhs
      }

    val lhsPartitions = Partitions(partitionedLhs(hotLhs), partitionedLhs(chillLhs))
    val rhsPartitions = Partitions(partitionedRhs(hotRhs), partitionedRhs(chillRhs))
    (lhsPartitions, rhsPartitions)
  }

  def partition[K, V, W](
    lhs: SCollection[(K, V)],
    rhs: SCollection[(K, W)],
    hotKeyCms: SCollection[TopCMS[K]]
  ): (Partitions[K, V], Partitions[K, W]) = {
    implicit val kCoder: Coder[K] = lhs.keyCoder
    implicit val vCoder: Coder[V] = lhs.valueCoder
    implicit val wCoder: Coder[W] = rhs.valueCoder

    val hotKeysSideInput = hotKeyCms.map(_.heavyHitters).asSingletonSideInput

    val (hotLhs, chillLhs) = (SideOutput[(K, V)](), SideOutput[(K, V)]())
    val (hotRhs, chillRhs) = (SideOutput[(K, W)](), SideOutput[(K, W)]())

    val partitionedLhs = lhs
      .withSideInputs(hotKeysSideInput)
      .transformWithSideOutputs(Seq(hotLhs, chillLhs), "Partition LHS") { case ((k, _), ctx) =>
        val hotKeys = ctx(hotKeysSideInput)
        if (hotKeys.contains(k)) hotLhs else chillLhs
      }

    val partitionedRhs = rhs
      .withSideInputs(hotKeysSideInput)
      .transformWithSideOutputs(Seq(hotRhs, chillRhs), "Partition RHS") { case ((k, _), ctx) =>
        val hotKeys = ctx(hotKeysSideInput)
        if (hotKeys.contains(k)) hotRhs else chillRhs
      }

    val lhsPartitions = Partitions(partitionedLhs(hotLhs), partitionedLhs(chillLhs))
    val rhsPartitions = Partitions(partitionedRhs(hotRhs), partitionedRhs(chillRhs))
    (lhsPartitions, rhsPartitions)
  }
}

private object SkewedJoins {

  private def union[T](hot: SCollection[T], chill: SCollection[T]): SCollection[T] =
    hot.withName("Union hot and chill join results").union(chill)

  def join[K, V, W](
    lhs: Partitions[K, V],
    rhs: Partitions[K, W]
  ): SCollection[(K, (V, W))] = {
    // Use hash join for hot keys
    val hotJoined = lhs.hot
      .withName("Hash join hot partitions")
      .hashJoin(rhs.hot)

    // Use regular join for the rest of the keys
    val chillJoined = lhs.chill
      .withName("Join chill partitions")
      .hashJoin(rhs.chill)

    union(hotJoined, chillJoined)
  }

  def leftOuterJoin[K, V, W](
    lhs: Partitions[K, V],
    rhs: Partitions[K, W]
  ): SCollection[(K, (V, Option[W]))] = {
    // Use hash join for hot keys
    val hotJoined = lhs.hot
      .withName("Hash left outer join hot partitions")
      .hashLeftOuterJoin(rhs.hot)

    // Use regular join for the rest of the keys
    val chillJoined = lhs.chill
      .withName("Left outer join chill partitions")
      .leftOuterJoin(rhs.chill)

    union(hotJoined, chillJoined)
  }

  def fullOuterJoin[K, V, W](
    lhs: Partitions[K, V],
    rhs: Partitions[K, W]
  ): SCollection[(K, (Option[V], Option[W]))] = {
    // Use hash join for hot keys
    val hotJoined = lhs.hot
      .withName("Hash full outer join hot partitions")
      .hashFullOuterJoin(rhs.hot)

    // Use regular join for the rest of the keys
    val chillJoined = lhs.chill
      .withName("Full outer join chill partitions")
      .fullOuterJoin(rhs.chill)

    union(hotJoined, chillJoined)
  }
}
