/*
 * Copyright 2018 Spotify AB.
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

import com.twitter.algebird.{CMS, CMSHasher}

private final case class Partitions[K, V](hot: SCollection[(K, V)], chill: SCollection[(K, V)])

 /**
  * Extra functions available on SCollections of (key, value) pairs for skwed joins
  * through an implicit conversion.
  *
  * @groupname cogroup CoGroup Operations
  * @groupname join Join Operations
  * @groupname per_key Per Key Aggregations
  * @groupname transform Transformations
  */
class PairSkewedSCollectionFunctions[K, V](val self: SCollection[(K, V)]) {

  // scalastyle:off parameter.number
   /**
    * N to 1 skew-proof flavor of [[PairSCollectionFunctions.join]].
    *
    * Perform a skewed join where some keys on the left hand may be hot, i.e. appear more than
    * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
    * estimate is within `eps * N` of the true frequency.
    * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
    * the left hand side stream so far.
    *
    * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
    * @example {{{
    * // Implicits that enabling CMS-hashing
    * import com.twitter.algebird.CMSHasherImplicits._
    *
    * val p = logs.skewedJoin(logMetadata)
    * }}}
    *
    * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
    * @group join
    * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
    *                        have inefficient `GroupByKey` implementation for groups with more than
    *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
    *                        10K, keep upper estimation error in mind. If you sample input via
    *                        `sampleFraction` make sure to adjust `hotKeyThreshold` accordingly.
    * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
    *            Must lie in `(0, 1)`.
    * @param seed A seed to initialize the random number generator used to create the pairwise
    *             independent hash functions.
    * @param delta A bound on the probability that a query estimate does not lie within some small
    *              interval (an interval that depends on `eps`) around the truth. Must lie in
    *              `(0, 1)`.
    * @param sampleFraction left side sample fraction. Default is `1.0` - no sampling.
    * @param withReplacement whether to use sampling with replacement, see
    *                        [[SCollection.sample(withReplacement:Boolean,fraction:Double)*
    *                        SCollection.sample]].
    */
  def skewedJoin[W: Coder](that: SCollection[(K, W)],
    hotKeyThreshold: Long = 9000,
    eps: Double = 0.001,
    seed: Int = 42,
    delta: Double = 1E-10,
    sampleFraction: Double = 1.0,
    withReplacement: Boolean = true)(
      implicit hasher: CMSHasher[K], koder: Coder[K], voder: Coder[V])
  : SCollection[(K, (V, W))] = {
    require(sampleFraction <= 1.0 && sampleFraction > 0.0,
      "Sample fraction has to be between (0.0, 1.0] - default is 1.0")

    import com.twitter.algebird._
    // Key aggregator for `k->#values`
    // TODO: might be better to use SparseCMS
    val keyAggregator = CMS.aggregator[K](eps, delta, seed)

    val leftSideKeys = if (sampleFraction < 1.0) {
      self.withName("Sample LHS").sample(withReplacement, sampleFraction).keys
    } else {
      self.keys
    }

    val cms = leftSideKeys.withName("Compute CMS of LHS keys").aggregate(keyAggregator)
    self.skewedJoin(that, hotKeyThreshold, cms)
  }
  // scalastyle:on parameter.number

   /**
    * N to 1 skew-proof flavor of [[PairSCollectionFunctions.join]].
    *
    * Perform a skewed join where some keys on the left hand may be hot, i.e. appear more than
    * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
    * estimate is within `eps * N` of the true frequency.
    * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
    * the left hand side stream so far.
    *
    * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
    * @example {{{
    * // Implicits that enabling CMS-hashing
    * import com.twitter.algebird.CMSHasherImplicits._
    *
    * val keyAggregator = CMS.aggregator[K](eps, delta, seed)
    * val hotKeyCMS = self.keys.aggregate(keyAggregator)
    * val p = logs.skewedJoin(logMetadata, hotKeyThreshold = 8500, cms=hotKeyCMS)
    * }}}
    *
    * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
    * @group join
    * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
    *                        have inefficient `GroupByKey` implementation for groups with more than
    *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
    *                        10K, keep upper estimation error in mind.
    * @param cms left hand side key [[com.twitter.algebird.CMSMonoid]]
    */
  def skewedJoin[W: Coder](that: SCollection[(K, W)],
    hotKeyThreshold: Long,
    cms: SCollection[CMS[K]])(implicit koder: Coder[K], voder: Coder[V])
  : SCollection[(K, (V, W))] = {
    val (selfPartitions, thatPartitions) = partitionInputs(that, hotKeyThreshold, cms)

    // Use hash join for hot keys
    val hotJoined = selfPartitions.hot
      .withName("Hash join hot partitions")
      .hashJoin(thatPartitions.hot)

    // Use regular join for the rest of the keys
    val chillJoined = selfPartitions.chill
      .withName("Join chill partitions")
      .join(thatPartitions.chill)

    hotJoined.withName("Union hot and chill join results") ++ chillJoined
  }

  // scalastyle:off parameter.number
   /**
    * N to 1 skew-proof flavor of [[PairSCollectionFunctions.leftOuterJoin]].
    *
    * Perform a skewed left join where some keys on the left hand may be hot, i.e. appear more than
    * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
    * estimate is within `eps * N` of the true frequency.
    * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
    * the left hand side stream so far.
    *
    * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
    * @example {{{
    * // Implicits that enabling CMS-hashing
    * import com.twitter.algebird.CMSHasherImplicits._
    *
    * val p = logs.skewedLeftJoin(logMetadata)
    * }}}
    *
    * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
    * @group join
    * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
    *                        have inefficient `GroupByKey` implementation for groups with more than
    *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
    *                        10K, keep upper estimation error in mind. If you sample input via
    *                        `sampleFraction` make sure to adjust `hotKeyThreshold` accordingly.
    * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
    *            Must lie in `(0, 1)`.
    * @param seed A seed to initialize the random number generator used to create the pairwise
    *             independent hash functions.
    * @param delta A bound on the probability that a query estimate does not lie within some small
    *              interval (an interval that depends on `eps`) around the truth. Must lie in
    *              `(0, 1)`.
    * @param sampleFraction left side sample fraction. Default is `1.0` - no sampling.
    * @param withReplacement whether to use sampling with replacement, see
    *                        [[SCollection.sample(withReplacement:Boolean,fraction:Double)*
    *                        SCollection.sample]].
    */
  def skewedLeftJoin[W: Coder](that: SCollection[(K, W)],
    hotKeyThreshold: Long = 9000,
    eps: Double = 0.001,
    seed: Int = 42,
    delta: Double = 1E-10,
    sampleFraction: Double = 1.0,
    withReplacement: Boolean = true)(
      implicit hasher: CMSHasher[K], koder: Coder[K], voder: Coder[V])
  : SCollection[(K, (V, Option[W]))] = {
    require(sampleFraction <= 1.0 && sampleFraction > 0.0,
      "Sample fraction has to be between (0.0, 1.0] - default is 1.0")

    import com.twitter.algebird._
    // Key aggregator for `k->#values`
    // TODO: might be better to use SparseCMS
    val keyAggregator = CMS.aggregator[K](eps, delta, seed)

    val leftSideKeys = if (sampleFraction < 1.0) {
      self.withName("Sample LHS").sample(withReplacement, sampleFraction).keys
    } else {
      self.keys
    }

    val cms = leftSideKeys.withName("Compute CMS of LHS keys").aggregate(keyAggregator)
    self.skewedLeftJoin(that, hotKeyThreshold, cms)
  }
  // scalastyle:on parameter.number

   /**
    * N to 1 skew-proof flavor of [[PairSCollectionFunctions.leftOuterJoin]].
    *
    * Perform a skewed left join where some keys on the left hand may be hot, i.e. appear more than
    * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
    * estimate is within `eps * N` of the true frequency.
    * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
    * the left hand side stream so far.
    *
    * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
    * @example {{{
    * // Implicits that enabling CMS-hashing
    * import com.twitter.algebird.CMSHasherImplicits._
    *
    * val keyAggregator = CMS.aggregator[K](eps, delta, seed)
    * val hotKeyCMS = self.keys.aggregate(keyAggregator)
    * val p = logs.skewedJoin(logMetadata, hotKeyThreshold = 8500, cms=hotKeyCMS)
    * }}}
    *
    * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
    * @group join
    * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
    *                        have inefficient `GroupByKey` implementation for groups with more than
    *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
    *                        10K, keep upper estimation error in mind.
    * @param cms left hand side key [[com.twitter.algebird.CMSMonoid]]
    */
  def skewedLeftJoin[W: Coder](that: SCollection[(K, W)],
    hotKeyThreshold: Long,
    cms: SCollection[CMS[K]])(implicit koder: Coder[K], voder: Coder[V])
  : SCollection[(K, (V, Option[W]))] = {
    val (selfPartitions, thatPartitions) = partitionInputs(that, hotKeyThreshold, cms)
    // Use hash join for hot keys
    val hotJoined = selfPartitions.hot
      .withName("Hash left join hot partitions")
      .hashLeftJoin(thatPartitions.hot)

    // Use regular join for the rest of the keys
    val chillJoined = selfPartitions.chill
      .withName("Left join chill partitions")
      .leftOuterJoin(thatPartitions.chill)

    hotJoined.withName("Union hot and chill join results") ++ chillJoined
  }

  // scalastyle:off parameter.number
   /**
    * N to 1 skew-proof flavor of [[PairSCollectionFunctions.fullOuterJoin]].
    *
    * Perform a skewed full join where some keys on the left hand may be hot, i.e. appear more than
    * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
    * estimate is within `eps * N` of the true frequency.
    * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
    * the left hand side stream so far.
    *
    * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
    * @example {{{
    * // Implicits that enabling CMS-hashing
    * import com.twitter.algebird.CMSHasherImplicits._
    *
    * val p = logs.skewedLeftJoin(logMetadata)
    * }}}
    *
    * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
    * @group join
    * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
    *                        have inefficient `GroupByKey` implementation for groups with more than
    *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
    *                        10K, keep upper estimation error in mind. If you sample input via
    *                        `sampleFraction` make sure to adjust `hotKeyThreshold` accordingly.
    * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
    *            Must lie in `(0, 1)`.
    * @param seed A seed to initialize the random number generator used to create the pairwise
    *             independent hash functions.
    * @param delta A bound on the probability that a query estimate does not lie within some small
    *              interval (an interval that depends on `eps`) around the truth. Must lie in
    *              `(0, 1)`.
    * @param sampleFraction left side sample fraction. Default is `1.0` - no sampling.
    * @param withReplacement whether to use sampling with replacement, see
    *                        [[SCollection.sample(withReplacement:Boolean,fraction:Double)*
    *                        SCollection.sample]].
    */
  def skewedFullOuterJoin[W: Coder](that: SCollection[(K, W)],
    hotKeyThreshold: Long = 9000,
    eps: Double = 0.001,
    seed: Int = 42,
    delta: Double = 1E-10,
    sampleFraction: Double = 1.0,
    withReplacement: Boolean = true)(
      implicit hasher: CMSHasher[K], koder: Coder[K], voder: Coder[V])
  : SCollection[(K, (Option[V], Option[W]))] = {
    require(sampleFraction <= 1.0 && sampleFraction > 0.0,
      "Sample fraction has to be between (0.0, 1.0] - default is 1.0")

    import com.twitter.algebird._
    // Key aggregator for `k->#values`
    // TODO: might be better to use SparseCMS
    val keyAggregator = CMS.aggregator[K](eps, delta, seed)

    val leftSideKeys = if (sampleFraction < 1.0) {
      self.withName("Sample LHS").sample(withReplacement, sampleFraction).keys
    } else {
      self.keys
    }

    val cms = leftSideKeys.withName("Compute CMS of LHS keys").aggregate(keyAggregator)
    self.skewedFullOuterJoin(that, hotKeyThreshold, cms)
  }

   /**
    * N to 1 skew-proof flavor of [[PairSCollectionFunctions.fullOuterJoin]].
    *
    * Perform a skewed full outer join where some keys on the left hand may be hot, i.e.appear
    * more than`hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta`
    * probability, and the estimate is within `eps * N` of the true frequency.
    * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
    * the left hand side stream so far.
    *
    * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
    * @example {{{
    * // Implicits that enabling CMS-hashing
    * import com.twitter.algebird.CMSHasherImplicits._
    *
    * val keyAggregator = CMS.aggregator[K](eps, delta, seed)
    * val hotKeyCMS = self.keys.aggregate(keyAggregator)
    * val p = logs.skewedJoin(logMetadata, hotKeyThreshold = 8500, cms=hotKeyCMS)
    * }}}
    *
    * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
    * @group join
    * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
    *                        have inefficient `GroupByKey` implementation for groups with more than
    *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
    *                        10K, keep upper estimation error in mind.
    * @param cms left hand side key [[com.twitter.algebird.CMSMonoid]]
    */
  def skewedFullOuterJoin[W: Coder](that: SCollection[(K, W)],
    hotKeyThreshold: Long,
    cms: SCollection[CMS[K]])(implicit koder: Coder[K], voder: Coder[V])
  : SCollection[(K, (Option[V], Option[W]))] = {
    val (selfPartitions, thatPartitions) = partitionInputs(that, hotKeyThreshold, cms)
    // Use hash join for hot keys
    val hotJoined = selfPartitions.hot
      .withName("Hash left join hot partitions")
      .hashFullOuterJoin(thatPartitions.hot)

    // Use regular join for the rest of the keys
    val chillJoined = selfPartitions.chill
      .withName("Left join chill partitions")
      .fullOuterJoin(thatPartitions.chill)

    hotJoined.withName("Union hot and chill join results") ++ chillJoined
  }

  private def partitionInputs[W: Coder](that: SCollection[(K, W)],
    hotKeyThreshold: Long,
    cms: SCollection[CMS[K]])(implicit koder: Coder[K], voder: Coder[V])
    : (Partitions[K, V], Partitions[K, W]) = {
    val (hotSelf, chillSelf) = (SideOutput[(K, V)](), SideOutput[(K, V)]())
    // scalastyle:off line.size.limit
    // Use asIterableSideInput as workaround for:
    // http://stackoverflow.com/questions/37126729/ismsinkwriter-expects-keys-to-be-written-in-strictly-increasing-order
    // scalastyle:on line.size.limit
    val keyCMS = cms.asIterableSideInput
    val error = cms
      .withName("Compute CMS error bound")
      .map(c => c.totalCount * c.eps).asSingletonSideInput

    val partitionedSelf = self
      .withSideInputs(keyCMS, error)
      .transformWithSideOutputs(Seq(hotSelf, chillSelf), "Partition LHS") { (e, c) =>
        if (c(keyCMS).nonEmpty &&
          c(keyCMS).head.frequency(e._1).estimate >= c(error) + hotKeyThreshold) {
          hotSelf
        } else {
          chillSelf
        }
      }

    val (hotThat, chillThat) = (SideOutput[(K, W)](), SideOutput[(K, W)]())
    val partitionedThat = that
      .withSideInputs(keyCMS, error)
      .transformWithSideOutputs(Seq(hotThat, chillThat), "Partition RHS") { (e, c) =>
        if (c(keyCMS).nonEmpty &&
          c(keyCMS).head.frequency(e._1).estimate >= c(error) + hotKeyThreshold) {
          hotThat
        } else {
          chillThat
        }
      }

    val selfPartitions = Partitions(partitionedSelf(hotSelf), partitionedSelf(chillSelf))
    val thatPartitions = Partitions(partitionedThat(hotThat), partitionedThat(chillThat))

    (selfPartitions, thatPartitions)
  }
}
