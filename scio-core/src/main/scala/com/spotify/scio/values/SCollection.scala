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

import java.io.PrintStream
import java.lang.{Boolean => JBoolean, Double => JDouble, Iterable => JIterable}
import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{AvroBytesUtil, BeamCoders, Coder, CoderMaterializer, WrappedBCoder}
import com.spotify.scio.estimators.{
  ApproxDistinctCounter,
  ApproximateUniqueCounter,
  ApproximateUniqueCounterByError
}
import com.spotify.scio.io._
import com.spotify.scio.schemas.{Schema, SchemaMaterializer, To}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util._
import com.spotify.scio.util.random.{BernoulliSampler, PoissonSampler}
import com.twitter.algebird.{Aggregator, Monoid, MonoidAggregator, Semigroup}
import org.apache.avro.file.CodecFactory
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.schemas.SchemaCoder
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment
import org.apache.beam.sdk.io.FileIO.Write.FileNaming
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{io => beam}
import org.joda.time.{Duration, Instant}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag
import scala.util.Try
import com.twitter.chill.ClosureCleaner

/** Convenience functions for creating SCollections. */
object SCollection {
  private[values] val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Create a union of multiple [[SCollection]] instances.
   * Will throw an exception if the provided iterable is empty.
   * For a version that accepts empty iterables, see [[ScioContext#unionAll]].
   */
  // `T: Coder` context bound is required since `scs` might be empty.
  def unionAll[T: Coder](scs: Iterable[SCollection[T]]): SCollection[T] =
    scs.head.context.unionAll(scs)

  /** Implicit conversion from SCollection to DoubleSCollectionFunctions. */
  implicit def makeDoubleSCollectionFunctions(s: SCollection[Double]): DoubleSCollectionFunctions =
    new DoubleSCollectionFunctions(s)

  /** Implicit conversion from SCollection to DoubleSCollectionFunctions. */
  implicit def makeDoubleSCollectionFunctions[T](
    s: SCollection[T]
  )(implicit num: Numeric[T]): DoubleSCollectionFunctions =
    new DoubleSCollectionFunctions(s.map(num.toDouble))

  /** Implicit conversion from SCollection to PairSCollectionFunctions. */
  implicit def makePairSCollectionFunctions[K, V](
    s: SCollection[(K, V)]
  ): PairSCollectionFunctions[K, V] =
    new PairSCollectionFunctions(s)

  implicit def makePairHashSCollectionFunctions[K, V](
    s: SCollection[(K, V)]
  ): PairHashSCollectionFunctions[K, V] =
    new PairHashSCollectionFunctions(s)

  implicit def makePairSkewedSCollectionFunctions[K, V](
    s: SCollection[(K, V)]
  ): PairSkewedSCollectionFunctions[K, V] =
    new PairSkewedSCollectionFunctions(s)

  final private[scio] case class State(postGbkOp: Boolean = false)
}

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
  self =>

  import TupleFunctions._

  // =======================================================================
  // States
  // =======================================================================

  private var _state: SCollection.State = SCollection.State()

  private[scio] def withState(f: SCollection.State => SCollection.State): SCollection[T] = {
    _state = f(_state)
    this
  }

  private[scio] def state: SCollection.State = _state

  // =======================================================================
  // Delegations for internal PCollection
  // =======================================================================

  /** A friendly name for this SCollection. */
  def name: String = internal.getName

  /** Assign a Coder to this SCollection. */
  def setCoder(coder: org.apache.beam.sdk.coders.Coder[T]): SCollection[T] = coder match {
    case wc: WrappedBCoder[T] => context.wrap(internal.setCoder(wc.u))
    case _                    => context.wrap(internal.setCoder(coder))
  }

  def setSchema(schema: Schema[T])(implicit ct: ClassTag[T]): SCollection[T] =
    if (!internal.hasSchema) {
      val (s, to, from) = SchemaMaterializer.materialize(schema)
      val td = TypeDescriptor.of(ScioUtil.classOf[T])
      try {
        context.wrap(internal.setSchema(s, td, to, from))
      } catch {
        case _: IllegalStateException =>
          // Coder has already been set
          map(identity)(Coder.beam(SchemaCoder.of(s, td, to, from)))
      }
    } else this

  private def ensureSerializable[A](coder: BCoder[A]): Either[Throwable, BCoder[A]] =
    coder match {
      case c if !context.isTest =>
        Right(c)
      // https://issues.apache.org/jira/browse/BEAM-5645
      case c if c.getClass.getPackage.getName.startsWith("org.apache.beam") =>
        Right(c)
      case _ =>
        Try[BCoder[A]](SerializableUtils.ensureSerializable(coder)).toEither
    }

  /**
   * Apply a [[org.apache.beam.sdk.transforms.PTransform PTransform]] and wrap the output in an
   * [[SCollection]].
   */
  def applyTransform[U: Coder](
    transform: PTransform[_ >: PCollection[T], PCollection[U]]
  ): SCollection[U] = applyTransform(tfName, transform)

  /**
   * Apply a [[org.apache.beam.sdk.transforms.PTransform PTransform]] and wrap the output in an
   * [[SCollection]].
   *
   * @param name  default transform name
   * @param transform [[org.apache.beam.sdk.transforms.PTransform PTransform]] to be applied
   */
  def applyTransform[U: Coder](
    name: String,
    transform: PTransform[_ >: PCollection[T], PCollection[U]]
  ): SCollection[U] = {
    val coder = CoderMaterializer.beam(context, Coder[U])
    ensureSerializable(coder).fold(throw _, pApply(name, transform).setCoder)
  }

  private[scio] def pApply[U](
    name: Option[String],
    transform: PTransform[_ >: PCollection[T], PCollection[U]]
  ): SCollection[U] = {
    val t =
      if (
        (classOf[Combine.Globally[T, U]] isAssignableFrom transform.getClass)
        && internal.getWindowingStrategy != WindowingStrategy.globalDefault()
      ) {
        // In case PCollection is windowed
        transform.asInstanceOf[Combine.Globally[T, U]].withoutDefaults()
      } else {
        transform
      }
    context.wrap(this.applyInternal(name, t))
  }

  private[scio] def pApply[U](
    transform: PTransform[_ >: PCollection[T], PCollection[U]]
  ): SCollection[U] =
    pApply(None, transform)

  private[scio] def pApply[U](
    name: String,
    transform: PTransform[_ >: PCollection[T], PCollection[U]]
  ): SCollection[U] =
    pApply(Option(name), transform)

  private[scio] def parDo[U: Coder](fn: DoFn[T, U]): SCollection[U] =
    this
      .pApply(ParDo.of(fn))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))

  /**
   * Apply a [[org.apache.beam.sdk.transforms.PTransform PTransform]] and wrap the output in an
   * [[SCollection]]. This is a special case of [[applyTransform]] for transforms with [[KV]]
   * output.
   */
  def applyKvTransform[K: Coder, V: Coder](
    transform: PTransform[_ >: PCollection[T], PCollection[KV[K, V]]]
  ): SCollection[KV[K, V]] =
    applyKvTransform(tfName, transform)

  /**
   * Apply a [[org.apache.beam.sdk.transforms.PTransform PTransform]] and wrap the output in an
   * [[SCollection]]. This is a special case of [[applyTransform]] for transforms with [[KV]]
   * output.
   *
   * @param name  default transform name
   * @param transform [[org.apache.beam.sdk.transforms.PTransform PTransform]] to be applied
   */
  def applyKvTransform[K: Coder, V: Coder](
    name: String,
    transform: PTransform[_ >: PCollection[T], PCollection[KV[K, V]]]
  ): SCollection[KV[K, V]] =
    applyTransform(name, transform)(Coder.raw(CoderMaterializer.kvCoder[K, V](context)))

  /** Apply a transform. */
  def transform[U](f: SCollection[T] => SCollection[U]): SCollection[U] = transform(this.tfName)(f)

  def transform[U](name: String)(f: SCollection[T] => SCollection[U]): SCollection[U] =
    context.wrap(transform_(name)(f(_).internal))

  private[scio] def transform_[U <: POutput](f: SCollection[T] => U): U =
    transform_(tfName)(f)

  private[scio] def transform_[U <: POutput](name: String)(f: SCollection[T] => U): U = {
    applyInternal(
      name,
      new PTransform[PCollection[T], U]() {
        override def expand(input: PCollection[T]): U = f(context.wrap(input))
      }
    )
  }

  /**
   * Go from an SCollection of type [[T]] to an SCollection of [[U]]
   * given the Schemas of both types [[T]] and [[U]].
   *
   * There are two constructors for [[To]]:
   *
   * Type safe (Schema compatibility is verified during compilation)
   * {{{
   *   SCollection[T]#to(To.safe[T, U])
   * }}}
   *
   * Unsafe conversion from [[T]] to [[U]]. Schema compatibility is not checked
   * during compile time.
   * {{{
   *   SCollection[T]#to[U](To.unsafe)
   * }}}
   */
  def to[U](to: To[T, U]): SCollection[U] = transform(to)

  // =======================================================================
  // Collection operations
  // =======================================================================

  /** lifts this [[SCollection]] to the specified type */
  def covary[U >: T]: SCollection[U] = this.asInstanceOf[SCollection[U]]

  /** lifts this [[SCollection]] to the specified type */
  def covary_[U](implicit ev: T <:< U): SCollection[U] = this.asInstanceOf[SCollection[U]]

  /** lifts this [[SCollection]] to the specified type */
  def contravary[U <: T]: SCollection[U] = this.asInstanceOf[SCollection[U]]

  /**
   * Convert this SCollection to an [[SCollectionWithFanout]] that uses an intermediate node to
   * combine parts of the data to reduce load on the final global combine step.
   * @param fanout the number of intermediate keys that will be used
   */
  def withFanout(fanout: Int): SCollectionWithFanout[T] =
    new SCollectionWithFanout[T](this, fanout)

  /**
   * Return the union of this SCollection and another one. Any identical elements will appear
   * multiple times (use [[distinct]] to eliminate them).
   * @group collection
   */
  def ++(that: SCollection[T]): SCollection[T] = this.union(that)

  /**
   * Return the union of this SCollection and another one. Any identical elements will appear
   * multiple times (use [[distinct]] to eliminate them).
   * @group collection
   */
  def union(that: SCollection[T]): SCollection[T] = {
    val o = PCollectionList
      .of(internal)
      .and(that.internal)
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
  def intersection(that: SCollection[T]): SCollection[T] =
    this.transform {
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
  def partition(numPartitions: Int, f: T => Int): Seq[SCollection[T]] = {
    require(numPartitions > 0, "Number of partitions should be positive")
    if (numPartitions == 1) {
      Seq(this)
    } else {
      this
        .applyInternal(Partition.of[T](numPartitions, Functions.partitionFn[T](f)))
        .getAll
        .iterator
        .asScala
        .map(context.wrap)
        .toSeq
    }
  }

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

  /**
   * Partition this SCollection into a map from possible key values to an SCollection of
   * corresponding elements based on the provided function .
   *
   * @param partitionKeys The keys for the output partitions
   * @param f function that assigns an output partition to each element, should be in the range
   * of `partitionKeys`
   * @return partitioned SCollections in a `Map`
   * @group collection
   */
  def partitionByKey[U](partitionKeys: Set[U])(f: T => U): Map[U, SCollection[T]] = {
    val partitionKeysIndexed = partitionKeys.toIndexedSeq

    partitionKeysIndexed
      .zip(partition(partitionKeys.size, (t: T) => partitionKeysIndexed.indexOf(f(t))))
      .toMap
  }

  /**
   * Partition this SCollection using Object.hashCode() into `n` partitions
   *
   * @param numPartitions number of output partitions
   * @return partitioned SCollections in a `Seq`
   * @group collection
   */
  def hashPartition(numPartitions: Int): Seq[SCollection[T]] =
    self.partition(
      numPartitions,
      t => Math.floorMod(t.hashCode(), numPartitions)
    )

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
  def aggregate[U: Coder](
    zeroValue: => U
  )(seqOp: (U, T) => U, combOp: (U, U) => U): SCollection[U] =
    this.pApply(Combine.globally(Functions.aggregateFn(context, zeroValue)(seqOp, combOp)))

  /**
   * Aggregate with [[com.twitter.algebird.Aggregator Aggregator]]. First each item `T` is mapped
   * to `A`, then we reduce with a [[com.twitter.algebird.Semigroup Semigroup]] of `A`, then
   * finally we present the results as `U`. This could be more powerful and better optimized in
   * some cases.
   * @group transform
   */
  def aggregate[A: Coder, U: Coder](aggregator: Aggregator[T, A, U]): SCollection[U] =
    this.transform { in =>
      val a = aggregator // defeat closure
      in.map(a.prepare).sum(a.semigroup).map(a.present)
    }

  /**
   * Aggregate with [[com.twitter.algebird.MonoidAggregator MonoidAggregator]]. First each item `T`
   * is mapped to `A`, then we reduce with a [[com.twitter.algebird.Monoid Monoid]] of `A`, then
   * finally we present the results as `U`. This could be more powerful and better optimized in
   * some cases.
   * @group transform
   */
  def aggregate[A: Coder, U: Coder](aggregator: MonoidAggregator[T, A, U]): SCollection[U] =
    this.transform { in =>
      val a = aggregator // defeat closure
      in.map(a.prepare).fold(a.monoid).map(a.present)
    }

  /**
   * Filter the elements for which the given `PartialFunction` is defined, and then map.
   * @group transform
   */
  def collect[U: Coder](pfn: PartialFunction[T, U]): SCollection[U] =
    this.transform {
      _.filter(pfn.isDefinedAt).map(pfn)
    }

  /**
   * Generic function to combine the elements using a custom set of aggregation functions. Turns
   * an `SCollection[T]` into a result of type `SCollection[C]`, for a "combined type" `C`. Note
   * that `T` and `C` can be different -- for example, one might combine an SCollection of type
   * `Int` into an SCollection of type `Seq[Int]`. Users provide three functions:
   *
   * - `createCombiner`, which turns a `T` into a `C` (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a `T` into a `C` (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two `C`'s into a single one.
   *
   * Both `mergeValue` and `mergeCombiners` are allowed to modify and return their first argument
   * instead of creating a new `U` to avoid memory allocation.
   *
   * @group transform
   */
  def combine[C: Coder](createCombiner: T => C)(
    mergeValue: (C, T) => C
  )(mergeCombiners: (C, C) => C): SCollection[C] = {
    SCollection.logger.warn(
      "combine/sum does not support default value and may fail in some streaming scenarios. " +
        "Consider aggregate/fold instead."
    )
    this.pApply(
      Combine
        .globally(Functions.combineFn(context, createCombiner, mergeValue, mergeCombiners))
        .withoutDefaults()
    )
  }

  /**
   * Count the number of elements in the SCollection.
   * @return a new SCollection with the count
   * @group transform
   */
  def count: SCollection[Long] =
    this.pApply(Count.globally[T]()).asInstanceOf[SCollection[Long]]

  /**
   * Count approximate number of distinct elements in the SCollection.
   * @param sampleSize the number of entries in the statistical sample; the higher this number, the
   * more accurate the estimate will be; should be `>= 16`
   * @group transform
   */
  def countApproxDistinct(sampleSize: Int): SCollection[Long] =
    ApproximateUniqueCounter(sampleSize).estimateDistinctCount(this)

  /**
   * Count approximate number of distinct elements in the SCollection.
   * @param maximumEstimationError the maximum estimation error, which should be in the range
   * `[0.01, 0.5]`
   * @group transform
   */
  def countApproxDistinct(maximumEstimationError: Double = 0.02): SCollection[Long] =
    ApproximateUniqueCounterByError(maximumEstimationError)
      .estimateDistinctCount(this)

  /**
   * Returns a single valued SCollection with estimated distinct count. Correctness is depends on the
   * [[ApproxDistinctCounter]] estimator.
   *
   * @Example
   * {{{
   *   val input: SCollection[T] = ...
   *   val distinctCount: SCollection[Long] = input.countApproxDistinct(ApproximateUniqueCounter(sampleSize))
   * }}}
   *
   * There are two different HLL++ implementations available in the `scio-extra` module.
   *  - [[com.spotify.scio.extra.hll.sketching.SketchHllPlusPlus]]
   *  - [[com.spotify.scio.extra.hll.zetasketch.ZetaSketchHllPlusPlus]]
   * @param estimator
   * @return
   */
  def countApproxDistinct(estimator: ApproxDistinctCounter[T]): SCollection[Long] =
    estimator.estimateDistinctCount(this)

  /**
   * Count of each unique value in this SCollection as an SCollection of (value, count) pairs.
   * @group transform
   */
  def countByValue: SCollection[(T, Long)] =
    this.transform {
      _.pApply(Count.perElement[T]()).map(TupleFunctions.klToTuple)
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
   * @param f The function to use to get representative values.
   * @tparam U The type of representative values used to dedup.
   * @group transform
   */
  // This is simplier than Distinct.withRepresentativeValueFn, and allows us to set Coders
  def distinctBy[U: Coder](f: T => U): SCollection[T] =
    this.transform { me =>
      me.keyBy(f).combineByKey(identity) { case (c, _) => c } { case (c, _) => c }.values
    }

  /**
   * Return a new SCollection containing only the elements that satisfy a predicate.
   * @group transform
   */
  def filter(f: T => Boolean): SCollection[T] =
    this.pApply(Filter.by(Functions.processFn(f.asInstanceOf[T => JBoolean])))

  /**
   * Return a new SCollection containing only the elements that don't satisfy a predicate.
   * @group transform
   */
  def filterNot(f: T => Boolean): SCollection[T] = filter(!f(_))

  /**
   * Return a new SCollection by first applying a function to all elements of
   * this SCollection, and then flattening the results.
   * @group transform
   */
  def flatMap[U: Coder](f: T => TraversableOnce[U]): SCollection[U] =
    this.parDo(Functions.flatMapFn(f))

  /**
   * Return a new `SCollection[U]` by flattening each element of an `SCollection[Traversable[U]]`.
   * @group transform
   */
  // Cannot use `U: Coder` context bound here because `U` depends on `ev`.
  def flatten[U](implicit ev: T => TraversableOnce[U], coder: Coder[U]): SCollection[U] =
    flatMap(ev)

  /**
   * Aggregate the elements using a given associative function and a neutral "zero value". The
   * function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   * @group transform
   */
  def fold(zeroValue: => T)(op: (T, T) => T): SCollection[T] =
    this.pApply(Combine.globally(Functions.aggregateFn(context, zeroValue)(op, op)))

  /**
   * Fold with [[com.twitter.algebird.Monoid Monoid]], which defines the associative function and
   * "zero value" for `T`. This could be more powerful and better optimized in some cases.
   * @group transform
   */
  def fold(implicit mon: Monoid[T]): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(context, mon)))

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
  def groupBy[K: Coder](f: T => K): SCollection[(K, Iterable[T])] =
    groupMap(f)(identity)

  /**
   * Return an SCollection of grouped items. Each group consists of a key and a sequence of
   * elements transformed into a value of type `U`. The ordering of elements within each group is not guaranteed,
   * and may even differ each time the resulting SCollection is evaluated.
   *
   * It is equivalent to groupBy(key).mapValues(_.map(f)), but more efficient.
   *
   * @group transform
   */
  def groupMap[K: Coder, U: Coder](f: T => K)(
    g: T => U
  ): SCollection[(K, Iterable[U])] =
    this.transform {
      val cf = ClosureCleaner.clean(f)
      val cg = ClosureCleaner.clean(g)

      (_: SCollection[T]).map(t => KV.of(cf(t), cg(t)))(Coder.raw(CoderMaterializer.kvCoder[K, U](context)))
        .pApply(GroupByKey.create[K, U]())
        .map(kvIterableToTuple)
    }

  /**
   * Return an SCollection of grouped items. Each group consists of a key and the result of an associative
   * reduce function. The ordering of elements within each group is not guaranteed, and may even differ each
   * time the resulting SCollection is evaluated.
   *
   * The associative function is performed locally on each mapper before sending results to
   * a reducer, similarly to a "combiner" in MapReduce
   *
   * @group transform
   */
  def groupMapReduce[K: Coder](f: T => K)(
    g: (T, T) => T
  ): SCollection[(K, T)] =
    this.transform {
      val cf = ClosureCleaner.clean(f)

      (_: SCollection[T]).map(t => KV.of(cf(t), t))(Coder.raw(CoderMaterializer.kvCoder[K, T](context)))
        .pApply(Combine.perKey(Functions.reduceFn(context, g)))
        .map(kvToTuple)
    }

  /**
   * Return a new SCollection containing only the elements that also exist in the `SideInput`.
   *
   * @group transform
   */
  def hashFilter(sideInput: SideInput[Set[T]]): SCollection[T] =
    self.map((_, ())).hashIntersectByKey(sideInput).keys

  /**
   * Create tuples of the elements in this SCollection by applying `f`.
   * @group transform
   */
  // Scala lambda is simpler than transforms.WithKeys
  def keyBy[K: Coder](f: T => K): SCollection[(K, T)] =
    this.map(v => (f(v), v))

  /**
   * Return a new SCollection by applying a function to all elements of this SCollection.
   * @group transform
   */
  def map[U: Coder](f: T => U): SCollection[U] = this.parDo(Functions.mapFn(f))

  /**
   * Return the max of this SCollection as defined by the implicit `Ordering[T]`.
   * @return a new SCollection with the maximum element
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Max
  def max(implicit ord: Ordering[T]): SCollection[T] =
    this.reduce(ord.max)

  /**
   * Return the mean of this SCollection as defined by the implicit `Numeric[T]`.
   * @return a new SCollection with the mean of elements
   * @group transform
   */
  def mean(implicit ev: Numeric[T]): SCollection[Double] = this.transform { in =>
    val e = ev // defeat closure
    in.map(e.toDouble)
      .asInstanceOf[SCollection[JDouble]]
      .pApply(Mean.globally())
      .asInstanceOf[SCollection[Double]]
  }

  /**
   * Return the min of this SCollection as defined by the implicit `Ordering[T]`.
   * @return a new SCollection with the minimum element
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Min
  def min(implicit ord: Ordering[T]): SCollection[T] =
    this.reduce(ord.min)

  /**
   * Compute the SCollection's data distribution using approximate `N`-tiles.
   * @return a new SCollection whose single value is an `Iterable` of the approximate `N`-tiles of
   * the elements
   * @group transform
   */
  def quantilesApprox(numQuantiles: Int)(implicit ord: Ordering[T]): SCollection[Iterable[T]] =
    this.transform {
      _.pApply(ApproximateQuantiles.globally(numQuantiles, ord))
        .map((_: JIterable[T]).asScala)
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
    val m = TreeMap(normalizedCumWeights.zipWithIndex: _*) // Map[lower bound, split]

    val sides = (1 until weights.length).map(_ => SideOutput[T]())
    val (head, tail) = this
      .withSideOutputs(sides: _*)
      .flatMap { (x, c) =>
        val i = m.to(ThreadLocalRandom.current().nextDouble()).last._2
        if (i == 0) {
          Seq(x) // Main output
        } else {
          c.output(sides(i - 1), x) // Side output
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
  def randomSplit(
    weightA: Double,
    weightB: Double
  ): (SCollection[T], SCollection[T], SCollection[T]) = {
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
    this.pApply(Combine.globally(Functions.reduceFn(context, op)).withoutDefaults())

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
  def sample(withReplacement: Boolean, fraction: Double): SCollection[T] =
    if (withReplacement) {
      this.parDo(new PoissonSampler[T](fraction))
    } else {
      this.parDo(new BernoulliSampler[T](fraction))
    }

  /**
   * Return an SCollection with the elements from `this` that are not in `other`.
   * @group transform
   */
  def subtract(that: SCollection[T]): SCollection[T] =
    this.transform {
      _.map((_, ())).subtractByKey(that).keys
    }

  /**
   * Reduce with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful and
   * better optimized than [[reduce]] in some cases.
   * @group transform
   */
  def sum(implicit sg: Semigroup[T]): SCollection[T] = {
    SCollection.logger.warn(
      "combine/sum does not support default value and may fail in some streaming scenarios. " +
        "Consider aggregate/fold instead."
    )
    this.pApply(Combine.globally(Functions.reduceFn(context, sg)).withoutDefaults())
  }

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
  def top(
    num: Int
  )(implicit ord: Ordering[T]): SCollection[Iterable[T]] =
    this.transform {
      _.pApply(Top.of(num, ord)).map((l: JIterable[T]) => l.asScala)
    }

  // =======================================================================
  // Hash operations
  // =======================================================================

  /**
   * Return the cross product with another SCollection by replicating `that` to all workers. The
   * right side should be tiny and fit in memory.
   * @group hash
   */
  def cross[U](that: SCollection[U]): SCollection[(T, U)] = {
    implicit val uCoder = that.coder
    this.transform { in =>
      val side = that.asListSideInput
      in.withSideInputs(side)
        .flatMap((t, s) => s(side).map((t, _)))
        .toSCollection
    }
  }

  /**
   * Look up values in an `SCollection[(T, V)]` for each element `T` in this SCollection by
   * replicating `that` to all workers. The right side should be tiny and fit in memory.
   * @group hash
   */
  def hashLookup[V](
    that: SCollection[(T, V)]
  ): SCollection[(T, Iterable[V])] = {
    implicit val vCoder = BeamCoders.getTupleCoders(that)._2
    this.transform { in =>
      val side = that.asMultiMapSingletonSideInput
      in.withSideInputs(side)
        .map((t, s) => (t, s(side).getOrElse(t, Iterable())))
        .toSCollection
    }
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
  def debug(
    out: () => PrintStream = () => Console.out,
    prefix: String = "",
    enabled: Boolean = true
  ): SCollection[T] =
    if (enabled) {
      tap(elem => out().println(prefix + elem))
    } else {
      this
    }

  /**
   * Applies f to each element of this [[SCollection]], and returns the original value.
   *
   * @group debug
   */
  def tap[U](f: T => U): SCollection[T] =
    map { elem => f(elem); elem }(Coder.beam(internal.getCoder))

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
   * Convert this SCollection to a [[SideInput]], mapping each window to a `Set[T]`, to be used
   * with [[withSideInputs]].
   *
   * The resulting [[SideInput]] is a one element singleton which is a `Set` of all elements in
   * the SCollection for the given window. The complete Set must fit in memory of the worker.
   *
   * @group side
   */
  // Find the distinct elements in parallel and then convert to a Set and SingletonSideInput.
  // This is preferred over aggregating as we want to map each window to a Set.
  def asSetSingletonSideInput: SideInput[Set[T]] =
    self
      .transform(
        _.distinct
          .groupBy(_ => ())
          .map[Set[T]](_._2.toSet)
      )
      .asSingletonSideInput(Set.empty[T])

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
   * val side4 = s4.asMultiMapSideInput
   *
   * val p: SCollection[MyRecord] = // ...
   * p.withSideInputs(side1, side2, side3).map { (x, s) =>
   *   // Extract side inputs from context
   *   val s1: Int = s(side1)
   *   val s2: Iterable[String] = s(side2)
   *   val s3: Map[String, Double] = s(side3)
   *   val s4: Map[String, Iterable[Double]] = s(side4)
   *   // ...
   * }
   * }}}
   * @group side
   */
  def withSideInputs(sides: SideInput[_]*): SCollectionWithSideInput[T] =
    new SCollectionWithSideInput[T](this, sides)

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
  def toWindowed: WindowedSCollection[T] =
    new WindowedSCollection[T](this)

  /**
   * Window values with the given function.
   * @group window
   */
  def withWindowFn[W <: BoundedWindow](
    fn: WindowFn[_ <: Any, W],
    options: WindowOptions = WindowOptions()
  ): SCollection[T] = {
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
    if (options.onTimeBehavior != null) {
      transform = transform.withOnTimeBehavior(options.onTimeBehavior)
    }

    this.pApply(transform)
  }

  /**
   * Window values into fixed windows.
   * @group window
   */
  def withFixedWindows(
    duration: Duration,
    offset: Duration = Duration.ZERO,
    options: WindowOptions = WindowOptions()
  ): SCollection[T] =
    this.withWindowFn(FixedWindows.of(duration).withOffset(offset), options)

  /**
   * Window values into sliding windows.
   * @group window
   */
  def withSlidingWindows(
    size: Duration,
    period: Duration = null,
    offset: Duration = Duration.ZERO,
    options: WindowOptions = WindowOptions()
  ): SCollection[T] = {
    var transform = SlidingWindows.of(size)
    if (period != null) {
      transform = transform.every(period)
    }
    transform = transform.withOffset(offset)
    this.withWindowFn(transform, options)
  }

  /**
   * Window values based on sessions.
   * @group window
   */
  def withSessionWindows(
    gapDuration: Duration,
    options: WindowOptions = WindowOptions()
  ): SCollection[T] =
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
  def windowByYears(number: Int, options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(CalendarWindows.years(number), options)

  /**
   * Window values into by months.
   * @group window
   */
  def windowByMonths(number: Int, options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(CalendarWindows.months(number), options)

  /**
   * Window values into by weeks.
   * @group window
   */
  def windowByWeeks(
    number: Int,
    startDayOfWeek: Int,
    options: WindowOptions = WindowOptions()
  ): SCollection[T] =
    this.withWindowFn(CalendarWindows.weeks(number, startDayOfWeek), options)

  /**
   * Window values into by days.
   * @group window
   */
  def windowByDays(number: Int, options: WindowOptions = WindowOptions()): SCollection[T] =
    this.withWindowFn(CalendarWindows.days(number), options)

  /**
   * Convert values into pairs of (value, window).
   * @group window
   */
  def withPaneInfo: SCollection[(T, PaneInfo)] =
    this.parDo(new DoFn[T, (T, PaneInfo)] {
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, (T, PaneInfo)]#ProcessContext): Unit =
        c.output((c.element(), c.pane()))
    })

  /**
   * Convert values into pairs of (value, timestamp).
   * @group window
   */
  def withTimestamp: SCollection[(T, Instant)] =
    this.parDo(new DoFn[T, (T, Instant)] {
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
  def withWindow[W <: BoundedWindow]: SCollection[(T, W)] =
    this
      .parDo(new DoFn[T, (T, BoundedWindow)] {
        @ProcessElement
        private[scio] def processElement(
          c: DoFn[T, (T, BoundedWindow)]#ProcessContext,
          window: BoundedWindow
        ): Unit =
          c.output((c.element(), window))
      })
      .asInstanceOf[SCollection[(T, W)]]

  /**
   * Assign timestamps to values.
   * With a optional skew
   * @group window
   */
  def timestampBy(f: T => Instant, allowedTimestampSkew: Duration = Duration.ZERO): SCollection[T] =
    this.applyTransform(
      WithTimestamps
        .of(Functions.serializableFn(f))
        .withAllowedTimestampSkew(allowedTimestampSkew)
    )

  // =======================================================================
  // Read operations
  // =======================================================================

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @return each line of the input files.
   * @see [[readFilesAsBytes]], [[readFilesAsString]]
   */
  def readFiles(implicit ev: T <:< String): SCollection[String] =
    readFiles(beam.TextIO.readFiles())

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @return each file fully read as [[Array[Byte]].
   * @see [[readFilesAsBytes]], [[readFilesAsString]]
   */
  def readFilesAsBytes(implicit ev: T <:< String): SCollection[Array[Byte]] =
    readFiles(_.readFullyAsBytes())

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @return each file fully read as [[String]].
   * @see [[readFilesAsBytes]], [[readFilesAsString]]
   */
  def readFilesAsString(implicit ev: T <:< String): SCollection[String] =
    readFiles(_.readFullyAsUTF8String())

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @see [[readFilesAsBytes]], [[readFilesAsString]]
   */
  def readFiles[A: Coder](
    f: beam.FileIO.ReadableFile => A
  )(implicit ev: T <:< String): SCollection[A] =
    readFiles(DirectoryTreatment.SKIP, Compression.AUTO)(f)

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @see [[readFilesAsBytes]], [[readFilesAsString]]
   *
   * @param directoryTreatment Controls how to handle directories in the input.
   * @param compression  Reads files using the given [[org.apache.beam.sdk.io.Compression]].
   */
  def readFiles[A: Coder](directoryTreatment: DirectoryTreatment, compression: Compression)(
    f: beam.FileIO.ReadableFile => A
  )(implicit ev: T <:< String): SCollection[A] = {
    val transform =
      ParDo
        .of(Functions.mapFn[beam.FileIO.ReadableFile, A](f))
        .asInstanceOf[PTransform[PCollection[beam.FileIO.ReadableFile], PCollection[A]]]
    readFiles(transform, directoryTreatment, compression)
  }

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @see [[readFilesAsBytes]], [[readFilesAsString]], [[readFiles]]
   *
   * @param directoryTreatment Controls how to handle directories in the input.
   * @param compression  Reads files using the given [[org.apache.beam.sdk.io.Compression]].
   */
  def readFiles[A: Coder](
    filesTransform: PTransform[PCollection[beam.FileIO.ReadableFile], PCollection[A]],
    directoryTreatment: DirectoryTreatment = DirectoryTreatment.SKIP,
    compression: Compression = Compression.AUTO
  )(implicit ev: T <:< String): SCollection[A] =
    if (context.isTest) {
      val id = context.testId.get
      this.flatMap(s => TestDataManager.getInput(id)(ReadIO(ev(s))).asIterable.get)
    } else {
      this
        .covary_[String]
        .applyTransform(new PTransform[PCollection[String], PCollection[A]]() {
          override def expand(input: PCollection[String]): PCollection[A] =
            input
              .apply(beam.FileIO.matchAll())
              .apply(
                beam.FileIO
                  .readMatches()
                  .withCompression(compression)
                  .withDirectoryTreatment(directoryTreatment)
              )
              .apply(filesTransform)
        })
    }

  /**
   * Pairs each element with the value of the provided [[SideInput]] in the element's window.
   *
   * Reify as List:
   * {{{
   *  val other: SCollection[Int] = sc.parallelize(Seq(1))
   *  val coll: SCollection[(Int, Seq[Int])] =
   *    sc.parallelize(Seq(1, 2))
   *      .reifySideInputAsValues(other.asListSideInput)
   * }}}
   *
   * Reify as Iterable:
   * {{{
   *  val other: SCollection[Int] = sc.parallelize(Seq(1))
   *  val coll: SCollection[(Int, Iterable[Int])] =
   *    sc.parallelize(Seq(1, 2))
   *      .reifySideInputAsValues(other.asIterableSideInput)
   * }}}
   *
   * Reify as Map:
   * {{{
   *  val other: SCollection[(Int, Int)] = sc.parallelize(Seq((1, 1)))
   *  val coll: SCollection[(Int, Map[Int, Int])] =
   *    sc.parallelize(Seq(1, 2))
   *      .reifySideInputAsValues(other.asMapSideInput)
   * }}}
   *
   * Reify as Multimap:
   * {{{
   *  val other: SCollection[(Int, Int)]  = sc.parallelize(Seq((1, 1)))
   *  val coll: SCollection[(Int, Map[Int, Iterable[Int]])]  =
   *    sc.parallelize(Seq(1, 2))
   *      .reifySideInputAsValues(other.asMultiMapSideInput)
   * }}}
   */
  // `U: Coder` context bound is required since `PCollectionView` may be of different type
  def reifySideInputAsValues[U: Coder](side: SideInput[U]): SCollection[(T, U)] =
    this.transform(_.withSideInputs(side).map((t, s) => (t, s(side))).toSCollection)

  /** Returns an [[SCollection]] consisting of a single `Seq[T]` element. */
  def reifyAsListInGlobalWindow: SCollection[Seq[T]] =
    reifyInGlobalWindow(_.asListSideInput)

  /** Returns an [[SCollection]] consisting of a single `Iterable[T]` element. */
  def reifyAsIterableInGlobalWindow: SCollection[Iterable[T]] =
    reifyInGlobalWindow(_.asIterableSideInput)

  /**
   * Returns an [[SCollection]] consisting of a single element, containing the value of the given
   * side input in the global window.
   *
   * Reify as List:
   * {{{
   *  val coll: SCollection[Seq[Int]] =
   *    sc.parallelize(Seq(1, 2)).reifyInGlobalWindow(_.asListSideInput)
   * }}}
   *
   * Can be used to replace patterns like:
   * {{{
   *  val coll: SCollection[Iterable[Int]] = sc.parallelize(Seq(1, 2)).groupBy(_ => ())
   * }}}
   * where you want to actually get an empty [[Iterable]] even if no data is present.
   */
  // `U: Coder` context bound is required since `PCollectionView` may be of different type
  private[scio] def reifyInGlobalWindow[U: Coder](
    view: SCollection[T] => SideInput[U]
  ): SCollection[U] =
    this.transform(coll =>
      context.parallelize[Unit](Seq(())).reifySideInputAsValues(view(coll)).values
    )

  // =======================================================================
  // Write operations
  // =======================================================================

  /**
   * Extract data from this SCollection as a closed [[Tap]]. The Tap will be available
   * once the pipeline completes successfully. `.materialize()` must be called before
   * the `ScioContext` is run, as its implementation modifies the current pipeline graph.
   *
   * {{{
   * val closedTap = sc.parallelize(1 to 10).materialize
   * sc.run().waitUntilDone().tap(closedTap)
   * }}}
   *
   * @group output
   */
  def materialize: ClosedTap[T] =
    materialize(ScioUtil.getTempFile(context), isCheckpoint = false)

  private[scio] def materialize(path: String, isCheckpoint: Boolean): ClosedTap[T] =
    if (context.isTest) {
      // Do not run assertions on materialized value but still access test context to trigger
      // the test checking if we're running inside a JobTest
      if (!isCheckpoint) TestDataManager.getOutput(context.testId.get)
      saveAsInMemoryTap
    } else {
      val elemCoder = CoderMaterializer.beam(context, coder)
      val schema = AvroBytesUtil.schema
      val avroCoder = Coder.avroGenericRecordCoder(schema)
      val write = beam.AvroIO
        .writeGenericRecords(schema)
        .to(ScioUtil.pathWithShards(path))
        .withSuffix(".obj.avro")
        .withCodec(CodecFactory.deflateCodec(6))
        .withMetadata(Map.empty[String, AnyRef].asJava)

      this
        .map(c => AvroBytesUtil.encode(elemCoder, c))(avroCoder)
        .applyInternal(write)
      ClosedTap(MaterializeTap[T](path, context))
    }

  private[scio] def textOut(
    path: String,
    suffix: String,
    numShards: Int,
    compression: Compression
  ) =
    beam.TextIO
      .write()
      .to(ScioUtil.pathWithShards(path))
      .withSuffix(suffix)
      .withNumShards(numShards)
      .withCompression(compression)

  /**
   * Save this SCollection as a text file. Note that elements must be of type `String`.
   * @group output
   */
  def saveAsTextFile(
    path: String,
    numShards: Int = TextIO.WriteParam.DefaultNumShards,
    suffix: String = TextIO.WriteParam.DefaultSuffix,
    compression: Compression = TextIO.WriteParam.DefaultCompression,
    header: Option[String] = TextIO.WriteParam.DefaultHeader,
    footer: Option[String] = TextIO.WriteParam.DefaultFooter,
    shardNameTemplate: String = TextIO.WriteParam.DefaultShardNameTemplate
  )(implicit ct: ClassTag[T]): ClosedTap[String] = {
    val s = if (classOf[String] isAssignableFrom ct.runtimeClass) {
      this.asInstanceOf[SCollection[String]]
    } else {
      this.map(_.toString)
    }
    s.write(TextIO(path))(
      TextIO.WriteParam(suffix, numShards, compression, header, footer, shardNameTemplate)
    )
  }

  /**
   * Save this SCollection as raw bytes. Note that elements must be of type `Array[Byte]`.
   * @group output
   */
  def saveAsBinaryFile(
    path: String,
    numShards: Int = BinaryIO.WriteParam.DefaultNumShards,
    prefix: String = BinaryIO.WriteParam.DefaultPrefix,
    suffix: String = BinaryIO.WriteParam.DefaultSuffix,
    compression: Compression = BinaryIO.WriteParam.DefaultCompression,
    header: Array[Byte] = BinaryIO.WriteParam.DefaultHeader,
    footer: Array[Byte] = BinaryIO.WriteParam.DefaultFooter,
    framePrefix: Array[Byte] => Array[Byte] = BinaryIO.WriteParam.DefaultFramePrefix,
    frameSuffix: Array[Byte] => Array[Byte] = BinaryIO.WriteParam.DefaultFrameSuffix,
    fileNaming: Option[FileNaming] = BinaryIO.WriteParam.DefaultFileNaming
  )(implicit ev: T <:< Array[Byte]): ClosedTap[Nothing] =
    this
      .covary_[Array[Byte]]
      .write(BinaryIO(path))(
        BinaryIO
          .WriteParam(
            prefix,
            suffix,
            numShards,
            compression,
            header,
            footer,
            framePrefix,
            frameSuffix,
            fileNaming
          )
      )

  /**
   * Save this SCollection with a custom output transform. The transform should have a unique name.
   * @group output
   */
  def saveAsCustomOutput[O <: POutput](
    name: String,
    transform: PTransform[PCollection[T], O]
  ): ClosedTap[Nothing] = {
    if (context.isTest) {
      TestDataManager.getOutput(context.testId.get)(CustomIO[T](name))(this)
    } else {
      this.internal.apply(name, transform)
    }

    ClosedTap[Nothing](EmptyTap)
  }

  private[scio] def saveAsInMemoryTap: ClosedTap[T] = {
    val tap = new InMemoryTap[T]
    InMemorySink.save(tap.id, this)
    ClosedTap(tap)
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
  def write(io: ScioIO[T])(params: io.WriteP): ClosedTap[io.tapT.T] =
    io.writeWithContext(this, params)

  def write(io: ScioIO[T] { type WriteP = Unit }): ClosedTap[io.tapT.T] =
    io.writeWithContext(this, ())
}

private[scio] class SCollectionImpl[T](val internal: PCollection[T], val context: ScioContext)
    extends SCollection[T] {}
