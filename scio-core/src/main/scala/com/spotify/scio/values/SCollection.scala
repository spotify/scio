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

// scalastyle:off file.size.limit

package com.spotify.scio.values

import java.io.PrintStream
import java.lang.{Boolean => JBoolean, Double => JDouble, Iterable => JIterable}
import java.util.concurrent.ThreadLocalRandom

import com.google.datastore.v1.Entity
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.io._
import com.spotify.scio.util._
import com.spotify.scio.util.random.{BernoulliSampler, PoissonSampler}
import com.twitter.algebird.{Aggregator, Monoid, Semigroup}
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.{Compression, FileBasedSink}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{io => beam}
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.concurrent._
import scala.reflect.ClassTag

/** Convenience functions for creating SCollections. */
object SCollection {

  /**
   * Create a union of multiple [[SCollection]] instances.
   * Will throw an exception if the provided iterable is empty.
   * For a version that accepts empty iterables, see [[ScioContext#unionAll]].
   */
  def unionAll[T: ClassTag](scs: Iterable[SCollection[T]]): SCollection[T] = {
    val o = PCollectionList
      .of(scs.map(_.internal).asJava)
      .apply("UnionAll", Flatten.pCollections())
    scs.head.context.wrap(o)
  }

  import scala.language.implicitConversions

  /** Implicit conversion from SCollection to DoubleSCollectionFunctions. */
  implicit def makeDoubleSCollectionFunctions(s: SCollection[Double]): DoubleSCollectionFunctions =
    new DoubleSCollectionFunctions(s)

  /** Implicit conversion from SCollection to DoubleSCollectionFunctions. */
  implicit def makeDoubleSCollectionFunctions[T](s: SCollection[T])(implicit num: Numeric[T])
  : DoubleSCollectionFunctions =
    new DoubleSCollectionFunctions(s.map(num.toDouble))

  /** Implicit conversion from SCollection to PairSCollectionFunctions. */
  implicit def makePairSCollectionFunctions[K: ClassTag, V: ClassTag](s: SCollection[(K, V)])
  : PairSCollectionFunctions[K, V] =
    new PairSCollectionFunctions(s)

  implicit def makePairHashSCollectionFunctions[K: ClassTag, V: ClassTag](s: SCollection[(K, V)])
  : PairHashSCollectionFunctions[K, V] =
    new PairHashSCollectionFunctions(s)

  implicit def makePairSkewedSCollectionFunctions[K: ClassTag, V: ClassTag](s: SCollection[(K, V)])
  : PairSkewedSCollectionFunctions[K, V] =
    new PairSkewedSCollectionFunctions(s)

}

// scalastyle:off number.of.methods
/**
 * A Scala wrapper for [[org.apache.beam.sdk.values.PCollection PCollection]]. Represents an
 * immutable, partitioned collection of elements that can be operated on in parallel. This class
 * contains the basic operations available on all SCollections, such as `map`, `filter`, and
 * `sum`. In addition, [[PairSCollectionFunctions]] contains operations available only on
 * SCollections of key-value pairs, such as `groupByKey` and `join`;
 * [[DoubleSCollectionFunctions]] contains operations available only on SCollections of `Double`s.
 *
 * @groupname collection Collection Operations
 * @groupname hash Hash Operations
 * @groupname output Output Sinks
 * @groupname side Side Input and Output Operations
 * @groupname transform Transformations
 * @groupname window Windowing Operations
 */
sealed trait SCollection[T] extends PCollectionWrapper[T] {

  import TupleFunctions._

  // =======================================================================
  // Delegations for internal PCollection
  // =======================================================================

  /** A friendly name for this SCollection. */
  def name: String = internal.getName

  /** Assign a Coder to this SCollection. */
  def setCoder(coder: Coder[T]): SCollection[T] = context.wrap(internal.setCoder(coder))

  /** Assign a name to this SCollection. */
  def setName(name: String): SCollection[T] = context.wrap(internal.setName(name))

  /**
   * Apply a [[org.apache.beam.sdk.transforms.PTransform PTransform]] and wrap the output in an
   * [[SCollection]].
   */
  def applyTransform[U: ClassTag](transform: PTransform[_ >: PCollection[T], PCollection[U]])
  : SCollection[U] = {
    val uCls = implicitly[ClassTag[U]].runtimeClass
    require(
      !(classOf[KV[_, _]] isAssignableFrom uCls),
      "Applying a transform with KV[K, V] output, use applyKvTransform instead")
    this.pApply(transform).setCoder(this.getCoder[U])
  }

  /**
   * Apply a [[org.apache.beam.sdk.transforms.PTransform PTransform]] and wrap the output in an
   * [[SCollection]]. This is a special case of [[applyTransform]] for transforms with [[KV]]
   * output.
   */
  def applyKvTransform[K: ClassTag, V: ClassTag]
  (transform: PTransform[_ >: PCollection[T], PCollection[KV[K, V]]])
  : SCollection[KV[K, V]] =
    this.pApply(transform).setCoder(this.getKvCoder[K, V])

  /** Apply a transform. */
  private[scio] def transform[U: ClassTag](f: SCollection[T] => SCollection[U])
  : SCollection[U] = {
    val o = internal.apply(this.tfName, new PTransform[PCollection[T], PCollection[U]]() {
      override def expand(input: PCollection[T]): PCollection[U] = {
        f(context.wrap(input)).internal
      }
    })
    context.wrap(o)
  }

  // =======================================================================
  // Collection operations
  // =======================================================================

  /**
   * Convert this SCollection to an [[SCollectionWithFanout]] that uses an intermediate node to
   * combine parts of the data to reduce load on the final global combine step.
   * @param fanout the number of intermediate keys that will be used
   */
  def withFanout(fanout: Int): SCollectionWithFanout[T] =
    new SCollectionWithFanout[T](internal, context, fanout)

  /**
   * Return the union of this SCollection and another one. Any identical elements will appear
   * multiple times (use [[distinct]] to eliminate them).
   * @group collection
   */
  // scalastyle:off method.name
  def ++(that: SCollection[T]): SCollection[T] = this.union(that)
  // scalastyle:on method.name

  /**
   * Return the union of this SCollection and another one. Any identical elements will appear
   * multiple times (use [[distinct]] to eliminate them).
   * @group collection
   */
  def union(that: SCollection[T]): SCollection[T] = {
    val o = PCollectionList
      .of(internal).and(that.internal)
      .apply(this.tfName, Flatten.pCollections())
    context.wrap(o)
  }

  /**
   * Return the intersection of this SCollection and another one. The output will not contain any
   * duplicate elements, even if the input SCollections did.
   *
   * Note that this method performs a shuffle internally.
   * @group collection
   */
  def intersection(that: SCollection[T]): SCollection[T] = this.transform {
    _.map((_, 1)).cogroup(that.map((_, 1))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.nonEmpty) Seq(t._1) else Seq.empty
    }
  }

  /**
   * Partition this SCollection with the provided function.
   *
   * @param numPartitions number of output partitions
   * @param f function that assigns an output partition to each element, should be in the range
   * `[0, numPartitions - 1]`
   * @return partitioned SCollections in a `Seq`
   * @group collection
   */
  def partition(numPartitions: Int, f: T => Int): Seq[SCollection[T]] =
    this.applyInternal(Partition.of[T](numPartitions, Functions.partitionFn[T](numPartitions, f)))
      .getAll.asScala.map(p => context.wrap(p))

  /**
   * Partition this SCollection into a pair of SCollections according to a predicate.
   *
   * @param p predicate on which to partition
   * @return a pair of SCollections: the first SCollection consists of all elements that satisfy
   * the predicate p and the second consists of all element that do not.
   * @group collection
   */
  def partition(p: T => Boolean): (SCollection[T], SCollection[T]) = {
    val Seq(left, right) = partition(2, t => if (p(t)) 0 else 1)
    (left, right)
  }


  // =======================================================================
  // Transformations
  // =======================================================================

  /**
   * Aggregate the elements using given combine functions and a neutral "zero value". This
   * function can return a different result type, `U`, than the type of this SCollection, `T`.
   * Thus, we need one operation for merging a `T` into an `U` and one operation for merging two
   * `U`'s. Both of these functions are allowed to modify and return their first argument instead
   * of creating a new `U` to avoid memory allocation.
   * @group transform
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U,
                                           combOp: (U, U) => U): SCollection[U] =
    this.pApply(Combine.globally(Functions.aggregateFn(zeroValue)(seqOp, combOp)))

  /**
   * Aggregate with [[com.twitter.algebird.Aggregator Aggregator]]. First each item `T` is mapped
   * to `A`, then we reduce with a [[com.twitter.algebird.Semigroup Semigroup]] of `A`, then
   * finally we present the results as `U`. This could be more powerful and better optimized in
   * some cases.
   * @group transform
   */
  def aggregate[A: ClassTag, U: ClassTag](aggregator: Aggregator[T, A, U])
  : SCollection[U] = this.transform { in =>
    val a = aggregator  // defeat closure
    in.map(a.prepare).sum(a.semigroup).map(a.present)
  }

  /**
   * Filter the elements for which the given `PartialFunction` is defined, and then map.
   * @group transform
   */
  def collect[U: ClassTag](pfn: PartialFunction[T, U]): SCollection[U] = this.transform {
    _.filter(pfn.isDefinedAt).map(pfn)
  }

  /**
   * Generic function to combine the elements using a custom set of aggregation functions. Turns
   * an `SCollection[T]` into a result of type `SCollection[C]`, for a "combined type" `C`. Note
   * that `V` and `C` can be different -- for example, one might combine an SCollection of type
   * `Int` into an SCollection of type `Seq[Int]`. Users provide three functions:
   *
   * - `createCombiner`, which turns a `V` into a `C` (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a `V` into a `C` (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two `C`'s into a single one.
   * @group transform
   */
  def combine[C: ClassTag](createCombiner: T => C)
                          (mergeValue: (C, T) => C)
                          (mergeCombiners: (C, C) => C): SCollection[C] =
    this.pApply(Combine.globally(Functions.combineFn(createCombiner, mergeValue, mergeCombiners)))

  /**
   * Count the number of elements in the SCollection.
   * @return a new SCollection with the count
   * @group transform
   */
  def count: SCollection[Long] = this.pApply(Count.globally[T]()).asInstanceOf[SCollection[Long]]

  /**
   * Count approximate number of distinct elements in the SCollection.
   * @param sampleSize the number of entries in the statistical sample; the higher this number, the
   * more accurate the estimate will be; should be `>= 16`
   * @group transform
   */
  def countApproxDistinct(sampleSize: Int): SCollection[Long] =
    this.pApply(ApproximateUnique.globally[T](sampleSize)).asInstanceOf[SCollection[Long]]

  /**
   * Count approximate number of distinct elements in the SCollection.
   * @param maximumEstimationError the maximum estimation error, which should be in the range
   * `[0.01, 0.5]`
   * @group transform
   */
  def countApproxDistinct(maximumEstimationError: Double = 0.02): SCollection[Long] =
    this
      .pApply(ApproximateUnique.globally[T](maximumEstimationError))
      .asInstanceOf[SCollection[Long]]

  /**
   * Count of each unique value in this SCollection as an SCollection of (value, count) pairs.
   * @group transform
   */
  def countByValue: SCollection[(T, Long)] = this.transform {
    _.pApply(Count.perElement[T]()).map(kvToTuple).asInstanceOf[SCollection[(T, Long)]]
  }

  /**
   * Return a new SCollection containing the distinct elements in this SCollection.
   * @group transform
   */
  def distinct: SCollection[T] = this.pApply(Distinct.create[T]())

  /**
   * Returns a new SCollection with distinct elements using given function to obtain a
   * representative value for each input element.
   *
   * @param f The funciton to use to get representative values.
   * @tparam U The type of representative values used to dedup.
   * @group transform
   */
  def distinctBy[U](f: T => U)(implicit ctu: ClassTag[U]): SCollection[T] =
    this.pApply(Distinct
      .withRepresentativeValueFn(Functions.serializableFn(f))
      .withRepresentativeType(TypeDescriptor.of(ctu.runtimeClass).asInstanceOf[TypeDescriptor[U]]))

  /**
   * Return a new SCollection containing only the elements that satisfy a predicate.
   * @group transform
   */
  def filter(f: T => Boolean): SCollection[T] =
    this.pApply(Filter.by(Functions.serializableFn(f.asInstanceOf[T => JBoolean])))

  /**
   * Return a new SCollection by first applying a function to all elements of
   * this SCollection, and then flattening the results.
   * @group transform
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): SCollection[U] =
    this.parDo(Functions.flatMapFn(f))

  /**
   * Return a new `SCollection[U]` by flattening each element of an `SCollection[Traversable[U]]`.
   * @group transform
   */
  def flatten[U: ClassTag](implicit ev: T => TraversableOnce[U]): SCollection[U] =
    flatMap(ev)

  /**
   * Aggregate the elements using a given associative function and a neutral "zero value". The
   * function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   * @group transform
   */
  def fold(zeroValue: T)(op: (T, T) => T): SCollection[T] =
    this.pApply(Combine.globally(Functions.aggregateFn(zeroValue)(op, op)))

  /**
   * Fold with [[com.twitter.algebird.Monoid Monoid]], which defines the associative function and
   * "zero value" for `T`. This could be more powerful and better optimized in some cases.
   * @group transform
   */
  def fold(implicit mon: Monoid[T]): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(mon)))

  /**
   * Return an SCollection of grouped items. Each group consists of a key and a sequence of
   * elements mapping to that key. The ordering of elements within each group is not guaranteed,
   * and may even differ each time the resulting SCollection is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using
   * [[PairSCollectionFunctions.aggregateByKey[U]* PairSCollectionFunctions.aggregateByKey]] or
   * [[PairSCollectionFunctions.reduceByKey]] will provide much better performance.
   * @group transform
   */
  def groupBy[K: ClassTag](f: T => K): SCollection[(K, Iterable[T])] = this.transform {
    _
      .pApply(WithKeys.of(Functions.serializableFn(f))).setCoder(this.getKvCoder[K, T])
      .pApply(GroupByKey.create[K, T]()).map(kvIterableToTuple)
  }

  /**
   * Create tuples of the elements in this SCollection by applying `f`.
   * @group transform
   */
  // Scala lambda is simpler than transforms.WithKeys
  def keyBy[K: ClassTag](f: T => K): SCollection[(K, T)] = this.map(v => (f(v), v))

  /**
   * Return a new SCollection by applying a function to all elements of this SCollection.
   * @group transform
   */
  def map[U: ClassTag](f: T => U): SCollection[U] = this.parDo(Functions.mapFn(f))

  /**
   * Return the max of this SCollection as defined by the implicit `Ordering[T]`.
   * @return a new SCollection with the maximum element
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Max
  def max(implicit ord: Ordering[T]): SCollection[T] = this.reduce(ord.max)

  /**
   * Return the mean of this SCollection as defined by the implicit `Numeric[T]`.
   * @return a new SCollection with the mean of elements
   * @group transform
   */
  def mean(implicit ev: Numeric[T]): SCollection[Double] = this.transform { in =>
    val e = ev  // defeat closure
    in
      .map(e.toDouble).asInstanceOf[SCollection[JDouble]]
      .pApply(Mean.globally()).asInstanceOf[SCollection[Double]]
  }

  /**
   * Return the min of this SCollection as defined by the implicit `Ordering[T]`.
   * @return a new SCollection with the minimum element
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Min
  def min(implicit ord: Ordering[T]): SCollection[T] = this.reduce(ord.min)

  /**
   * Compute the SCollection's data distribution using approximate `N`-tiles.
   * @return a new SCollection whose single value is an `Iterable` of the approximate `N`-tiles of
   * the elements
   * @group transform
   */
  def quantilesApprox(numQuantiles: Int)
                     (implicit ord: Ordering[T]): SCollection[Iterable[T]] = this.transform {
    _
      .pApply(ApproximateQuantiles.globally(numQuantiles, ord))
      .map(_.asInstanceOf[JIterable[T]].asScala)
  }

  /**
   * Randomly splits this SCollection with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @return split SCollections in an array
   * @group transform
   */
  def randomSplit(weights: Array[Double]): Array[SCollection[T]] = {
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    val m = TreeMap(normalizedCumWeights.zipWithIndex: _*)  // Map[lower bound, split]

    val sides = (1 until weights.length).map(_ => SideOutput[T]())
    val (head, tail) = this
      .withSideOutputs(sides: _*)
      .flatMap { (x, c) =>
        val i = m.to(ThreadLocalRandom.current().nextDouble()).last._2
        if (i == 0) {
          Seq(x)  // Main output
        } else {
          c.output(sides(i - 1), x)  // Side output
          Nil
        }
      }
    (head +: sides.map(tail(_))).toArray
  }

  /**
   * Randomly splits this SCollection into two parts.
   *
   * @param weight weight for left hand side SCollection, should be in the range `(0, 1)`
   * @return split SCollections in a Tuple2
   * @group transform
   */
  def randomSplit(weight: Double): (SCollection[T], SCollection[T]) = {
    require(weight > 0.0 && weight < 1.0)
    val splits = randomSplit(Array(weight, 1d - weight))
    (splits(0), splits(1))
  }

  /**
   * Randomly splits this SCollection into three parts.
   * Note: `0 < weightA + weightB < 1`
   *
   * @param weightA weight for first SCollection, should be in the range `(0, 1)`
   * @param weightB weight for second SCollection, should be in the range `(0, 1)`
   * @return split SCollections in a Tuple3
   * @group transform
   */
  def randomSplit(weightA: Double, weightB: Double):
  (SCollection[T], SCollection[T], SCollection[T]) = {
    require(weightA > 0.0 && weightB > 0.0 && (weightA + weightB) < 1.0)
    val splits = randomSplit(Array(weightA, weightB, 1d - (weightA + weightB)))
    (splits(0), splits(1), splits(2))
  }

  /**
   * Reduce the elements of this SCollection using the specified commutative and associative
   * binary operator.
   * @group transform
   */
  def reduce(op: (T, T) => T): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(op)))

  /**
   * Return a sampled subset of this SCollection.
   * @return a new SCollection whose single value is an `Iterable` of the samples
   * @group transform
   */
  def sample(sampleSize: Int): SCollection[Iterable[T]] = this.transform {
    _.pApply(Sample.fixedSizeGlobally(sampleSize)).map(_.asScala)
  }

  /**
   * Return a sampled subset of this SCollection.
   * @group transform
   */
  def sample(withReplacement: Boolean, fraction: Double): SCollection[T] = {
    if (withReplacement) {
      this.parDo(new PoissonSampler[T](fraction))
    } else {
      this.parDo(new BernoulliSampler[T](fraction))
    }
  }

  /**
   * Return an SCollection with the elements from `this` that are not in `other`.
   * @group transform
   */
  def subtract(that: SCollection[T]): SCollection[T] = this.transform {
    _.map((_, ())).subtractByKey(that).keys
  }

  /**
   * Reduce with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful and
   * better optimized than [[reduce]] in some cases.
   * @group transform
   */
  def sum(implicit sg: Semigroup[T]): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(sg)))

  /**
   * Return a sampled subset of any `num` elements of the SCollection.
   * @group transform
   */
  def take(num: Long): SCollection[T] = this.pApply(Sample.any(num))

  /**
   * Return the top k (largest) elements from this SCollection as defined by the specified
   * implicit `Ordering[T]`.
   * @return a new SCollection whose single value is an `Iterable` of the top k
   * @group transform
   */
  def top(num: Int)(implicit ord: Ordering[T]): SCollection[Iterable[T]] = this.transform {
    _.pApply(Top.of(num, ord)).map(_.asInstanceOf[JIterable[T]].asScala)
  }

  // =======================================================================
  // Hash operations
  // =======================================================================

  /**
   * Return the cross product with another SCollection by replicating `that` to all workers. The
   * right side should be tiny and fit in memory.
   * @group hash
   */
  def cross[U: ClassTag](that: SCollection[U]): SCollection[(T, U)] = this.transform { in =>
    // TODO: switch to ListSideInput when https://github.com/spotify/scio/issues/1152 is resolved
    val side = that.aggregate(Aggregator.toList[U]).asSingletonSideInput
    in
      .withSideInputs(side)
      .flatMap((t, s) => s(side).map((t, _)))
      .toSCollection
  }

  /**
   * Look up values in an `SCollection[(T, V)]` for each element `T` in this SCollection by
   * replicating `that` to all workers. The right side should be tiny and fit in memory.
   * @group hash
   */
  def hashLookup[V: ClassTag](that: SCollection[(T, V)])
  : SCollection[(T, Iterable[V])] = this.transform { in =>
    val side = that.asMultiMapSideInput
    in
      .withSideInputs(side)
      .map((t, s) => (t, s(side).getOrElse(t, Iterable())))
      .toSCollection
  }

  /**
   * Print content of an SCollection to `out()`.
   * @param out where to write the debug information. Default: stdout
   * @param prefix prefix for each logged entry. Default: empty string
   * @param enabled if debugging is enabled or not. Default: true.
   *                It can be useful to set this to sc.isTest to avoid
   *                debugging when running in production.
   * @group debug
   */
  def debug(out: () => PrintStream = () => Console.out, prefix: String = "",
            enabled: Boolean = true): SCollection[T] =
    if (enabled) {
      this.filter { e =>
        // scalastyle:off regex
        out().println(prefix + e)
        // scalastyle:on regex
        // filter that never removes
        true
      }
    } else {
      this
    }

  // =======================================================================
  // Side input operations
  // =======================================================================

  /**
   * Convert this SCollection of a single value per window to a [[SideInput]], to be used with
   * [[withSideInputs]].
   * @group side
   */
  def asSingletonSideInput: SideInput[T] =
    new SingletonSideInput[T](this.applyInternal(View.asSingleton()))

  /**
   * Convert this SCollection of a single value per window to a [[SideInput]] with a default value,
   * to be used with [[withSideInputs]].
   * @group side
   */
  def asSingletonSideInput(defaultValue: T): SideInput[T] =
    new SingletonSideInput[T](this.applyInternal(View.asSingleton().withDefaultValue(defaultValue)))

  /**
   * Convert this SCollection to a [[SideInput]], mapping each window to a `Seq`, to be used with
   * [[withSideInputs]].
   *
   * The resulting `Seq` is required to fit in memory.
   * @group side
   */
  // j.u.List#asScala returns s.c.mutable.Buffer which has an O(n) .toList method
  // returning Seq[T] here to avoid copying
  def asListSideInput: SideInput[Seq[T]] =
    new ListSideInput[T](this.applyInternal(View.asList()))

  /**
   * Convert this SCollection to a [[SideInput]], mapping each window to an `Iterable`, to be used
   * with [[withSideInputs]].
   *
   * The values of the `Iterable` for a window are not required to fit in memory, but they may also
   * not be effectively cached. If it is known that every window fits in memory, and stronger
   * caching is desired, use [[asListSideInput]].
   * @group side
   */
  def asIterableSideInput: SideInput[Iterable[T]] =
    new IterableSideInput[T](this.applyInternal(View.asIterable()))

  /**
   * Convert this SCollection to an [[SCollectionWithSideInput]] with one or more [[SideInput]]s,
   * similar to Spark broadcast variables. Call [[SCollectionWithSideInput.toSCollection]] when
   * done with side inputs.
   *
   * {{{
   * val s1: SCollection[Int] = // ...
   * val s2: SCollection[String] = // ...
   * val s3: SCollection[(String, Double)] = // ...
   *
   * // Prepare side inputs
   * val side1 = s1.asSingletonSideInput
   * val side2 = s2.asIterableSideInput
   * val side3 = s3.asMapSideInput
   *
   * val p: SCollection[MyRecord] = // ...
   * p.withSideInputs(side1, side2, side3).map { (x, s) =>
   *   // Extract side inputs from context
   *   val s1: Int = s(side1)
   *   val s2: Iterable[String] = s(side2)
   *   val s3: Map[String, Iterable[Double]] = s(side3)
   *   // ...
   * }
   * }}}
   * @group side
   */
  def withSideInputs(sides: SideInput[_]*): SCollectionWithSideInput[T] =
    new SCollectionWithSideInput[T](internal, context, sides)

  // =======================================================================
  // Side output operations
  // =======================================================================

  /**
   * Convert this SCollection to an [[SCollectionWithSideOutput]] with one or more
   * [[SideOutput]]s, so that a single transform can write to multiple destinations.
   *
   * {{{
   * // Prepare side inputs
   * val side1 = SideOutput[String]()
   * val side2 = SideOutput[Int]()
   *
   * val p: SCollection[MyRecord] = // ...
   * p.withSideOutputs(side1, side2).map { (x, s) =>
   *   // Write to side outputs via context
   *   s.output(side1, "word").output(side2, 1)
   *   // ...
   * }
   * }}}
   * @group side
   */
  def withSideOutputs(sides: SideOutput[_]*): SCollectionWithSideOutput[T] =
    new SCollectionWithSideOutput[T](internal, context, sides)

  // =======================================================================
  // Windowing operations
  // =======================================================================

  /**
   * Convert this SCollection to an [[WindowedSCollection]].
   * @group window
   */
  def toWindowed: WindowedSCollection[T] = new WindowedSCollection[T](internal, context)

  /**
   * Window values with the given function.
   * @group window
   */
  def withWindowFn[W <: BoundedWindow](fn: WindowFn[AnyRef, W],
                                       options: WindowOptions = WindowOptions())
  : SCollection[T] = {
    var transform = Window.into(fn).asInstanceOf[Window[T]]
    if (options.trigger != null) {
      transform = transform.triggering(options.trigger)
    }
    if (options.accumulationMode != null) {
      if (options.accumulationMode == AccumulationMode.ACCUMULATING_FIRED_PANES) {
        transform = transform.accumulatingFiredPanes()
      } else if (options.accumulationMode == AccumulationMode.DISCARDING_FIRED_PANES) {
        transform = transform.discardingFiredPanes()
      } else {
        throw new RuntimeException(s"Unsupported accumulation mode ${options.accumulationMode}")
      }
    }
    if (options.allowedLateness != null) {
      transform = if (options.closingBehavior == null) {
        transform.withAllowedLateness(options.allowedLateness)
      } else {
        transform.withAllowedLateness(options.allowedLateness, options.closingBehavior)
      }
    }
    if (options.timestampCombiner != null) {
      transform = transform.withTimestampCombiner(options.timestampCombiner)
    }
    this.pApply(transform)
  }

  /**
   * Window values into fixed windows.
   * @group window
   */
  def withFixedWindows(duration: Duration,
                       offset: Duration = Duration.ZERO,
                       options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(FixedWindows.of(duration).withOffset(offset), options)

  /**
   * Window values into sliding windows.
   * @group window
   */
  def withSlidingWindows(size: Duration,
                         period: Duration = null,
                         offset: Duration = Duration.ZERO,
                         options: WindowOptions = WindowOptions()): SCollection[T] = {
    var transform = SlidingWindows.of(size).withOffset(offset)
    if (period != null) {
      transform = transform.every(period)
    }
    this.withWindowFn(transform, options)
  }

  /**
   * Window values based on sessions.
   * @group window
   */
  def withSessionWindows(gapDuration: Duration,
                         options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(Sessions.withGapDuration(gapDuration), options)

  /**
   * Group values in to a single global window.
   * @group window
   */
  def withGlobalWindow(options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(new GlobalWindows(), options)

  /**
   * Window values into by years.
   * @group window
   */
  def windowByYears(number: Int,
                    options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(CalendarWindows.years(number), options)

  /**
   * Window values into by months.
   * @group window
   */
  def windowByMonths(number: Int,
                     options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(CalendarWindows.months(number), options)

  /**
   * Window values into by weeks.
   * @group window
   */
  def windowByWeeks(number: Int, startDayOfWeek: Int,
                    options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(CalendarWindows.weeks(number, startDayOfWeek), options)

  /**
   * Window values into by days.
   * @group window
   */
  def windowByDays(number: Int,
                   options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(CalendarWindows.days(number), options)

  /**
   * Convert values into pairs of (value, window).
   * @group window
   */
  def withPaneInfo: SCollection[(T, PaneInfo)] = this.parDo(new DoFn[T, (T, PaneInfo)] {
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, (T, PaneInfo)]#ProcessContext): Unit =
      c.output((c.element(), c.pane()))
  })

  /**
   * Convert values into pairs of (value, timestamp).
   * @group window
   */
  def withTimestamp: SCollection[(T, Instant)] = this.parDo(new DoFn[T, (T, Instant)] {
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, (T, Instant)]#ProcessContext): Unit =
      c.output((c.element(), c.timestamp()))
  })

  /**
   * Convert values into pairs of (value, window).
   * @tparam W window type, must be
   *           [[org.apache.beam.sdk.transforms.windowing.BoundedWindow BoundedWindow]] or one of
   *           it's sub-types, e.g.
   *           [[org.apache.beam.sdk.transforms.windowing.GlobalWindow GlobalWindow]] if this
   *           SCollection is not windowed or
   *           [[org.apache.beam.sdk.transforms.windowing.IntervalWindow IntervalWindow]] if it is
   *           windowed.
   * @group window
   */
  def withWindow[W <: BoundedWindow]: SCollection[(T, W)] = this.parDo(
    new DoFn[T, (T, BoundedWindow)] {
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, (T, BoundedWindow)]#ProcessContext,
                                       window: BoundedWindow): Unit =
        c.output((c.element(), window))
    }).asInstanceOf[SCollection[(T, W)]]

  /**
   * Assign timestamps to values.
   * With a optional skew
   * @group window
   */
  def timestampBy(f: T => Instant, allowedTimestampSkew: Duration = Duration.ZERO): SCollection[T] =
    this.applyTransform(WithTimestamps.of(Functions.serializableFn(f))
      .withAllowedTimestampSkew(allowedTimestampSkew))

  // =======================================================================
  // Write operations
  // =======================================================================

  /**
   * Extract data from this SCollection as a `Future`. The `Future` will be completed once the
   * pipeline completes successfully.
   * @group output
   */
  def materialize: Future[Tap[T]] = materialize(ScioUtil.getTempFile(context), isCheckpoint = false)
  private[scio] def materialize(path: String, isCheckpoint: Boolean): Future[Tap[T]] =
    if (context.isTest) {
      // Do not run assertions on materialized value but still access test context to trigger
      // the test checking if we're running inside a JobTest
      if (!isCheckpoint) context.testOutput
      saveAsInMemoryTap
    } else {
      val elemCoder = this.getCoder[T]
      val write = beam.AvroIO.writeGenericRecords(AvroBytesUtil.schema)
        .to(this.pathWithShards(path))
        .withSuffix(".obj.avro")
        .withCodec(CodecFactory.deflateCodec(6))
        .withMetadata(Map.empty[String, AnyRef].asJava)
      this
        .parDo(new DoFn[T, GenericRecord] {
          @ProcessElement
          private[scio] def processElement(c: DoFn[T, GenericRecord]#ProcessContext): Unit =
            c.output(AvroBytesUtil.encode(elemCoder, c.element()))
        })
        .applyInternal(write)
      context.makeFuture(MaterializeTap[T](path))
    }

  private[scio] def pathWithShards(path: String) = path.replaceAll("\\/+$", "") + "/part"

  private[scio] def textOut(path: String,
                            suffix: String,
                            numShards: Int,
                            compression: Compression) = {
    beam.TextIO.write()
      .to(pathWithShards(path))
      .withSuffix(suffix)
      .withNumShards(numShards)
      .withWritableByteChannelFactory(FileBasedSink.CompressionType.fromCanonical(compression))
  }

  /**
   * Save this SCollection as a Datastore dataset. Note that elements must be of type `Entity`.
   * @group output
   */
  def saveAsDatastore(projectId: String)(implicit ev: T <:< Entity): Future[Tap[Entity]] =
    this.asInstanceOf[SCollection[Entity]].write(DatastoreIO(projectId))

  /**
   * Save this SCollection as a Pub/Sub topic.
   * @group output
   */
  def saveAsPubsub(topic: String,
                   idAttribute: String = null,
                   timestampAttribute: String = null)
  : Future[Tap[T]] =
    this.write(PubsubIO[T](topic))

  /**
    * Save this SCollection as a Pub/Sub topic using the given map as message attributes.
    * @group output
    */
  def saveAsPubsubWithAttributes[V: ClassTag](topic: String,
                                              idAttribute: String = null,
                                              timestampAttribute: String = null)
                                             (implicit ev: T <:< (V, Map[String, String]))
  : Future[Tap[(V, Map[String, String])]] = {
    val io = PubsubIO.withAttributes[V](topic, idAttribute, timestampAttribute)
    this.asInstanceOf[SCollection[(V, Map[String, String])]].write(io)
  }

  /**
   * Save this SCollection as a text file. Note that elements must be of type `String`.
   * @group output
   */
  def saveAsTextFile(path: String,
                     suffix: String = ".txt",
                     numShards: Int = 0,
                     compression: Compression = Compression.UNCOMPRESSED)
  : Future[Tap[String]] = {
    val s = if (classOf[String] isAssignableFrom this.ct.runtimeClass) {
      this.asInstanceOf[SCollection[String]]
    } else {
      this.map(_.toString)
    }
    s.write(TextIO(path))(TextIO.WriteParam(suffix, numShards, compression))
  }

  /**
   * Save this SCollection with a custom output transform. The transform should have a unique name.
   * @group output
   */
  def saveAsCustomOutput[O <: POutput](name: String, transform: PTransform[PCollection[T], O])
  : Future[Tap[T]] = {
    if (context.isTest) {
      context.testOut(CustomIO[T](name))(this)
    } else {
      this.internal.apply(name, transform)
    }
    Future.failed(new NotImplementedError("Custom future not implemented"))
  }

  private[scio] def saveAsInMemoryTap: Future[Tap[T]] = {
    val tap = InMemoryTap[T]()
    InMemorySink.save(tap.id, this)
    context.makeFuture(tap)
  }

  /**
   * Generic write method for all `ScioIO[T]` implementations, if it is test pipeline this will
   * evaluate pre-registered output IO implementation which match for the passing `ScioIO[T]`
   * implementation. if not this will invoke [[com.spotify.scio.io.ScioIO[T]#write]] method along
   * with write configurations passed by.
   *
   * @param io     an implementation of `ScioIO[T]` trait
   * @param params configurations need to pass to perform underline write implementation
   */
  def write(io: ScioIO[T])(params: io.WriteP): Future[Tap[T]] =
    writeImpl(io)(params)

  private def writeImpl(io: ScioIO[T])(params: io.WriteP): Future[Tap[T]] = {
    if (context.isTest) {
      context.testOut(io)(this)
      this.saveAsInMemoryTap
    } else {
      io.write(this, params)
    }
  }

  // scalastyle:off structural.type
  def write(io: ScioIO[T]{ type WriteP = Unit }): Future[Tap[T]] =
    writeImpl(io)(())
  // scalastyle:on structural.type

}
// scalastyle:on number.of.methods

private[scio] class SCollectionImpl[T: ClassTag](val internal: PCollection[T],
                                                 val context: ScioContext)
  extends SCollection[T] {
  val ct: ClassTag[T] = implicitly[ClassTag[T]]
}

// scalastyle:on file.size.limit
