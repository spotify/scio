package com.spotify.cloud.dataflow.values

import java.io.File
import java.lang.{Boolean => JBoolean, Double => JDouble, Iterable => JIterable}

import com.google.api.services.bigquery.model.{TableSchema, TableReference, TableRow}
import com.google.api.services.datastore.DatastoreV1.Entity
import com.google.cloud.dataflow.sdk.coders.{TableRowJsonCoder, Coder}
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.{WriteDisposition, CreateDisposition}
import com.google.cloud.dataflow.sdk.io.{
  AvroIO => GAvroIO,
  BigQueryIO => GBigQueryIO,
  DatastoreIO => GDatastoreIO,
  PubsubIO => GPubsubIO,
  TextIO => GTextIO
}
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.{
  ApproximateQuantiles, ApproximateUnique, Combine, Count, DoFn, Filter, Flatten, GroupByKey, Mean, Partition,
  RemoveDuplicates, Sample, Top, View, WithKeys
}
import com.google.cloud.dataflow.sdk.transforms.windowing._
import com.google.cloud.dataflow.sdk.values._
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.testing._
import com.spotify.cloud.dataflow.util._
import com.twitter.algebird.{Aggregator, Monoid, Semigroup}
import org.apache.avro.Schema
import org.apache.avro.generic.{IndexedRecord, GenericRecord}
import org.apache.avro.specific.SpecificRecord
import org.joda.time.{Instant, Duration}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Convenience functions for creating SCollections. */
object SCollection {

  /** Create a new SCollection from a PCollection. */
  def apply[T: ClassTag](p: PCollection[T])(implicit context: DataflowContext): SCollection[T] =
    new SCollectionImpl(p)

  /** Create a union of multiple SCollections */
  def unionAll[T: ClassTag](scs: Iterable[SCollection[T]]): SCollection[T] = {
    val o = PCollectionList.of(scs.map(_.internal).asJava).apply(Flatten.pCollections().withName(CallSites.getCurrent))
    new SCollectionImpl(o)(scs.head.context, scs.head.ct)
  }

}

/** Scala wrapper for PCollection. */
sealed trait SCollection[T] extends PCollectionWrapper[T] {

  import TupleFunctions._

  // =======================================================================
  // Delegations for internal PCollection
  // =======================================================================

  /** A friendly name for this SCollection. */
  def name: String = internal.getName

  /** Assign a Coder to this SCollection. */
  def setCoder(coder: Coder[T]): SCollection[T] = SCollection(internal.setCoder(coder))

  /** Assign a name to this SCollection. */
  def setName(name: String): SCollection[T] = SCollection(internal.setName(name))

  // =======================================================================
  // Collection operations
  // =======================================================================

  /**
   * Return the union of this SCollection and another one. Any identical elements will appear
   * multiple times (use `.distinct()` to eliminate them).
   */
  def ++(that: SCollection[T]): SCollection[T] = this.union(that)

  /**
   * Return the union of this SCollection and another one. Any identical elements will appear
   * multiple times (use `.distinct()` to eliminate them).
   */
  def union(that: SCollection[T]): SCollection[T] = {
    val o = PCollectionList
      .of(internal).and(that.internal)
      .apply(Flatten.pCollections().withName(CallSites.getCurrent))
    SCollection(o)
  }

  /**
   * Return the intersection of this SCollection and another one. The output will not contain any
   * duplicate elements, even if the input SCollections did.
   *
   * Note that this method performs a shuffle internally.
   */
  def intersection(that: SCollection[T]): SCollection[T] =
    this.map((_, 1)).coGroup(that.map((_, 1))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.nonEmpty) Seq(t._1) else Seq.empty
    }

  /**
   * Partition this SCollection with the provided function.
   *
   * @param numPartitions number of output partitions
   * @param f function that assigns an output partition to each element, should be in the range
   * `[0, numPartitions - 1]`
   * @return partitioned SCollections in a Seq
   */
  def partition(numPartitions: Int, f: T => Int): Seq[SCollection[T]] =
    this.applyInternal(Partition.of[T](numPartitions, Functions.partitionFn[T](numPartitions, f)))
      .getAll.asScala.map(p => SCollection(p))

  // =======================================================================
  // Transformations
  // =======================================================================

  /**
   * Aggregate the elements using given combine functions and a neutral "zero value". This
   * function can return a different result type, U, than the type of this SCollection, T. Thus,
   * we need one operation for merging a T into an U and one operation for merging two U's. Both
   * of these functions are allowed to modify and return their first argument instead of creating
   * a new U to avoid memory allocation.
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): SCollection[U] =
    this.apply(Combine.globally(Functions.aggregateFn(zeroValue)(seqOp, combOp)))

  /**
   * Aggregate with [[com.twitter.algebird.Aggregator]]. First each item T is mapped to A, then we
   * reduce with a semigroup of A, then finally we present the results as U. This could be more
   * powerful and better optimized in some cases.
   */
  def aggregate[A: ClassTag, U: ClassTag](aggregator: Aggregator[T, A, U]): SCollection[U] =
    this.map(aggregator.prepare).sum()(aggregator.semigroup).map(aggregator.present)

  /**
   * Generic function to combine the elements using a custom set of aggregation functions. Turns
   * an SCollection[T] into a result of type SCollection[C], for a "combined type" C. Note that V
   * and C can be different -- for example, one might combine an SCollection of type Int into an
   * SCollection of type Seq[Int]. Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two C's into a single one.
   */
  def combine[C: ClassTag](createCombiner: T => C)
                          (mergeValue: (C, T) => C)
                          (mergeCombiners: (C, C) => C): SCollection[C] =
    this.apply(Combine.globally(Functions.combineFn(createCombiner, mergeValue, mergeCombiners)))

  /**
   * Count the number of elements in the SCollection.
   * @return a new SCollection with the count
   */
  def count(): SCollection[Long] = this.apply(Count.globally[T]()).asInstanceOf[SCollection[Long]]

  /**
   * Count approximate number of distinct elements in the SCollection.
   * @param sampleSize the number of entries in the statisticalsample; the higher this number, the
   * more accurate the estimate will be; should be `>= 16`
   */
  def countApproxDistinct(sampleSize: Int): SCollection[Long] =
    this.apply(ApproximateUnique.globally[T](sampleSize)).asInstanceOf[SCollection[Long]]

  /**
   * Count approximate number of distinct elements in the SCollection.
   * @param maximumEstimationError the maximum estimation error, which should be in the range
   * `[0.01, 0.5]`
   */
  def countApproxDistinct(maximumEstimationError: Double = 0.02): SCollection[Long] =
    this.apply(ApproximateUnique.globally[T](maximumEstimationError)).asInstanceOf[SCollection[Long]]

  /** Count of each unique value in this SCollection as an SCollection of (value, count) pairs. */
  def countByValue(): SCollection[(T, Long)] =
    this.apply(Count.perElement[T]()).map(kvToTuple).asInstanceOf[SCollection[(T, Long)]]

  /** Return a new SCollection containing the distinct elements in this SCollection. */
  def distinct(): SCollection[T] = this.apply(RemoveDuplicates.create[T]())

  /** Return a new SCollection containing only the elements that satisfy a predicate. */
  def filter(f: T => Boolean): SCollection[T] =
    this.apply(Filter.by(Functions.serializableFn(f.asInstanceOf[T => JBoolean])))

  /**
   * Return a new SCollection by first applying a function to all elements of
   * this SCollection, and then flattening the results.
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): SCollection[U] = this.parDo(Functions.flatMapFn(f))

  /**
   * Aggregate the elements using a given associative function and a neutral "zero value". The
   * function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   */
  def fold(zeroValue: T)(op: (T, T) => T): SCollection[T] =
    this.apply(Combine.globally(Functions.aggregateFn(zeroValue)(op, op)))

  /**
   * Fold with [[com.twitter.algebird.Monoid]], which defines the associative function and "zero
   * value" for T. This could be more powerful and better optimized in some cases.
   */
  def fold(implicit mon: Monoid[T]): SCollection[T] = this.apply(Combine.globally(Functions.reduceFn(mon)))

  /**
   * Return an SCollection of grouped items. Each group consists of a key and a sequence of
   * elements mapping to that key. The ordering of elements within each group is not guaranteed,
   * and may even differ each time the resulting SCollection is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using
   * [[PairSCollectionFunctions.aggregateByKey[U]* PairSCollectionFunctions.aggregateByKey]] or
   * [[PairSCollectionFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K: ClassTag](f: T => K): SCollection[(K, Iterable[T])] =
    this
      .apply(WithKeys.of(Functions.serializableFn(f))).setCoder(this.getKvCoder[K, T])
      .apply(GroupByKey.create[K, T]()).map(kvIterableToTuple)

  /** Create tuples of the elements in this SCollection by applying `f`. */
  // Scala lambda is simpler than transforms.WithKeys
  def keyBy[K: ClassTag](f: T => K): SCollection[(K, T)] = this.map(v => (f(v), v))

  /** Return a new SCollection by applying a function to all elements of this SCollection. */
  def map[U: ClassTag](f: T => U): SCollection[U] = this.parDo(Functions.mapFn(f))

  /**
   * Return the max of this SCollection as defined by the implicit Ordering[T].
   * @return a new SCollection with the maximum element
   */
  // Scala lambda is simpler and more powerful than transforms.Max
  def max()(implicit ord: Ordering[T]): SCollection[T] = this.reduce(ord.max)

  /**
   * Return the mean of this SCollection as defined by the implicit Numeric[T].
   * @return a new SCollection with the mean of elements
   */
  def mean()(implicit ev: Numeric[T]): SCollection[Double] = {
    val o = this
      .map(ev.toDouble).asInstanceOf[SCollection[JDouble]]
      .applyInternal(Mean.globally()).asInstanceOf[PCollection[Double]]
    SCollection(o)
  }

  /**
   * Return the min of this SCollection as defined by the implicit Ordering[T].
   * @return a new SCollection with the minimum element
   */
  // Scala lambda is simpler and more powerful than transforms.Min
  def min()(implicit ord: Ordering[T]): SCollection[T] = this.reduce(ord.min)

  /**
   * Compute the SCollection's data distribution using approximate `N`-tiles.
   * @return a new SCollection whose single value is an Iterable of the approximate `N`-tiles of
   * the elements
   */
  def quantilesApprox(numQuantiles: Int)(implicit ord: Ordering[T]): SCollection[Iterable[T]] =
    this.apply(ApproximateQuantiles.globally(numQuantiles, ord)).map(_.asInstanceOf[JIterable[T]].asScala)

  /**
   * Reduce the elements of this SCollection using the specified commutative and associative
   * binary operator.
   */
  def reduce(op: (T, T) => T): SCollection[T] = this.apply(Combine.globally(Functions.reduceFn(op)))

  /**
   * Return a sampled subset of this SCollection.
   * @return a new SCollection whose single value is an Iterable of the
   * samples
   */
  def sample(sampleSize: Int): SCollection[Iterable[T]] =
    this.apply(Sample.fixedSizeGlobally(sampleSize)).map(_.asScala)

  // TODO: implement sample by fraction, with and without replacement.

  /** Return an SCollection with the elements from `this` that are not in `other`. */
  def subtract(that: SCollection[T]): SCollection[T] =
    this.map((_, 1)).coGroup(that.map((_, 1))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.isEmpty) Seq(t._1) else Seq.empty
    }

  /**
   * Reduce with [[com.twitter.algebird.Semigroup]]. This could be more powerful and better
   * optimized in some cases.
   */
  def sum()(implicit sg: Semigroup[T]): SCollection[T] = this.apply(Combine.globally(Functions.reduceFn(sg)))

  /** Return a sampled subset of any `num` elements of the SCollection. */
  def take(num: Long): SCollection[T] = this.apply(Sample.any(num))

  /**
   * Return the top k (largest) elements from this SCollection as defined by the specified
   * implicit Ordering[T].
   * @return a new SCollection whose single value is an Iterable of the top k
   */
  def top(num: Int)(implicit ord: Ordering[T]): SCollection[Iterable[T]] =
    this.apply(Top.of(num, ord)).map(_.asInstanceOf[JIterable[T]].asScala)

  // =======================================================================
  // Hash operations
  // =======================================================================

  /**
   * Return the cross product with another SCollection by replicating `that` to all workers. The
   * right side should be tiny and fit in memory.
   */
  def cross[U: ClassTag](that: SCollection[U]): SCollection[(T, U)] = {
    val side = that.asIterableSideInput
    this
      .withSideInputs(side)
      .flatMap((t, s) => s(side).map((t, _)))
      .toSCollection
  }

  /**
   * Look up values in a SCollection[(T, V)] for each element T in this SCollection by replicating
   * `that` to all workers. The right side should be tiny and fit in memory.
   */
  def hashLookup[V: ClassTag](that: SCollection[(T, V)]): SCollection[(T, Iterable[V])] = {
    val side = that.asMapSideInput
    this
      .withSideInputs(side)
      .map((t, s) => (t, s(side).getOrElse(t, Iterable())))
      .toSCollection
  }

  // =======================================================================
  // Accumulator operations
  // =======================================================================

  /**
   * Convert this SCollection to an enhanced [[SCollectionWithAccumulator]] that provides access
   * to an [[Accumulator]] for some transforms, similar to Hadoop counters. Call
   * [[SCollectionWithAccumulator.toSCollection]] when done with accumulators.
   * For example, mapping over an SCollection of integers and accumulating sum, max, and min.
   *
   * {{{
   * val p: SCollection[Int] = // ...
   * p.withAccumulator.map { (x, acc) =>
   *   acc.add("sum", x)
   *      .max("max", x)
   *      .min("min", x)
   *   x
   * }
   * .toSCollection
   * }}}
   */
  def withAccumulator: SCollectionWithAccumulator[T] = new SCollectionWithAccumulator[T](internal)

  // =======================================================================
  // Side input operations
  // =======================================================================

  def asSingletonSideInput: SideInput[T] = new SingletonSideInput[T](this.applyInternal(View.asSingleton()))

  def asIterableSideInput: SideInput[Iterable[T]] = new IterableSideInput[T](this.applyInternal(View.asIterable()))

  def withSideInputs(sides: SideInput[_]*): SCollectionWithSideInput[T] =
    new SCollectionWithSideInput[T](internal, sides)

  // =======================================================================
  // Side output operations
  // =======================================================================

  def withSideOutputs(sides: SideOutput[_]*): SCollectionWithSideOutput[T] =
    new SCollectionWithSideOutput[T](internal, sides)

  // =======================================================================
  // Windowing operations
  // =======================================================================

  def toWindowed: WindowedSCollection[T] = new WindowedSCollection[T](internal)

  def withWindowFn(fn: WindowFn[T, _]): SCollection[T] = this.apply(Window.into(fn))

  def withFixedWindows(duration: Duration, offset: Duration = Duration.ZERO): SCollection[T] =
    this.withWindowFn(FixedWindows.of(duration).withOffset(offset).asInstanceOf[WindowFn[T, _]])

  def withSlidingWindows(size: Duration,
                         period: Duration = Duration.millis(1),
                         offset: Duration = Duration.ZERO): SCollection[T] =
    this.withWindowFn(SlidingWindows.of(size).every(period).withOffset(offset).asInstanceOf[WindowFn[T, _]])

  def withSessionWindows(gapDuration: Duration): SCollection[T] =
    this.withWindowFn(Sessions.withGapDuration(gapDuration).asInstanceOf[WindowFn[T, _]])

  def withGlobalWindow(): SCollection[T] = this.withWindowFn(new GlobalWindows().asInstanceOf[WindowFn[T, _]])

  def windowByYears(number: Int): SCollection[T] =
    this.withWindowFn(CalendarWindows.years(number).asInstanceOf[WindowFn[T, _]])

  def windowByMonths(number: Int): SCollection[T] =
    this.withWindowFn(CalendarWindows.months(number).asInstanceOf[WindowFn[T, _]])

  def windowByWeeks(number: Int, startDayOfWeek: Int): SCollection[T] =
    this.withWindowFn(CalendarWindows.weeks(number, startDayOfWeek).asInstanceOf[WindowFn[T, _]])

  def windowByDay(number: Int): SCollection[T] =
    this.withWindowFn(CalendarWindows.days(number).asInstanceOf[WindowFn[T, _]])

  def withTimestamp(): SCollection[(T, Instant)] = this.parDo(new DoFn[T, (T, Instant)] {
    override def processElement(c: DoFn[T, (T, Instant)]#ProcessContext): Unit = {
      c.output((c.element(), c.timestamp()))
    }
  })

  def timestampBy(f: T => Instant): SCollection[T] = this.parDo(FunctionsWithWindowedValue.timestampFn(f))

  // =======================================================================
  // Write operations
  // =======================================================================

  private def pathWithShards(path: String) = {
    if (this.context.pipeline.getRunner.isInstanceOf[DirectPipelineRunner]) {
      val f = new File(path)
      if (f.exists()) {
        throw new RuntimeException(s"Output directory $path already exists")
      }
      f.mkdirs()
    }
    path.replaceAll("\\/+$", "") + "/part"
  }

  private def avroOut(path: String, numShards: Int) =
    GAvroIO.Write.to(pathWithShards(path)).withNumShards(numShards).withSuffix(".avro")

  private def textOut(path: String, suffix: String, numShards: Int) =
    GTextIO.Write.to(pathWithShards(path)).withNumShards(numShards).withSuffix(suffix)

  private def tableRowJsonOut(path: String, numShards: Int) =
    textOut(path, ".json", numShards).withCoder(TableRowJsonCoder.of())

  /** Save this SCollection as an Avro file. Note that elements must be of type IndexedRecord. */
  def saveAsAvroFile(path: String, numShards: Int = 0)(implicit ev: T <:< IndexedRecord): Unit =
    if (context.isTest) {
      context.testOut(AvroIO(path))(internal)
    } else {
      val transform = avroOut(path, numShards)
      if (classOf[GenericRecord] isAssignableFrom ct.runtimeClass) {
        val schema = ct.runtimeClass.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
        this
          .asInstanceOf[SCollection[GenericRecord]]
          .applyInternal(transform.withSchema(schema))
      } else if (classOf[SpecificRecord] isAssignableFrom ct.runtimeClass) {
        this.applyInternal(transform.withSchema(ct.runtimeClass.asInstanceOf[Class[T]]))
      } else {
        throw new RuntimeException(s"${ct.runtimeClass} is not supported")
      }
    }

  /** Save this SCollection as a Bigquery table. Note that elements must be of type TableRow. */
  def saveAsBigQuery(table: TableReference, schema: TableSchema,
                     createDisposition: CreateDisposition,
                     writeDisposition: WriteDisposition)
                    (implicit ev: T <:< TableRow): Unit = {
    val tableSpec = GBigQueryIO.toTableSpec(table)
    if (context.isTest) {
      context.testOut(BigQueryIO(tableSpec))(internal.asInstanceOf[PCollection[TableRow]])
    } else {
      var transform = GBigQueryIO.Write.to(table)
      if (schema != null) transform = transform.withSchema(schema)
      if (createDisposition != null) transform = transform.withCreateDisposition(createDisposition)
      if (writeDisposition != null) transform = transform.withWriteDisposition(writeDisposition)
      this.asInstanceOf[SCollection[TableRow]].applyInternal(transform)
    }
  }

  /** Save this SCollection as a Bigquery table. Note that elements must be of type TableRow. */
  def saveAsBigQuery(tableSpec: String, schema: TableSchema = null,
                     createDisposition: CreateDisposition = null,
                     writeDisposition: WriteDisposition = null)
                    (implicit ev: T <:< TableRow): Unit =
    saveAsBigQuery(GBigQueryIO.parseTableSpec(tableSpec), schema, createDisposition, writeDisposition)

  /** Save this SCollection as a Datastore dataset. Note that elements must be of type Entity. */
  def saveAsDatastore(datasetId: String)(implicit ev: T <:< Entity): Unit =
    if (context.isTest) {
      context.testOut(DatastoreIO(datasetId))(internal.asInstanceOf[PCollection[Entity]])
    } else {
      this.asInstanceOf[SCollection[Entity]].applyInternal(GDatastoreIO.writeTo(datasetId))
    }

  /** Save this SCollection as a Pub/Sub topic. Note that elements must be of type String. */
  def saveAsPubsub(topic: String)(implicit ev: T <:< String): Unit =
    if (context.isTest) {
      context.testOut(PubsubIO(topic))(internal.asInstanceOf[PCollection[String]])
    } else {
      this.asInstanceOf[SCollection[String]].applyInternal(GPubsubIO.Write.topic(topic))
    }

  /** Save this SCollection as a JSON text file. Note that elements must be of type TableRow. */
  def saveAsTableRowJsonFile(path: String, numShards: Int = 0)(implicit ev: T <:< TableRow): Unit =
    if (context.isTest) {
      context.testOut(BigQueryIO(path))(internal.asInstanceOf[PCollection[TableRow]])
    } else {
      this.asInstanceOf[SCollection[TableRow]].applyInternal(tableRowJsonOut(path, numShards))
    }

  /** Save this SCollection as a text file. Note that elements must be of type String. */
  def saveAsTextFile(path: String, suffix: String = ".txt", numShards: Int = 0)(implicit ev: T <:< String): Unit =
    if (context.isTest) {
      context.testOut(TextIO(path))(internal.asInstanceOf[PCollection[String]])
    } else {
      this.asInstanceOf[SCollection[String]].applyInternal(textOut(path, suffix, numShards))
    }

}

private class SCollectionImpl[T](val internal: PCollection[T])
                                (implicit val context: DataflowContext, val ct: ClassTag[T])
  extends SCollection[T] {}
