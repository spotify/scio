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
  ApproximateQuantiles, ApproximateUnique, Combine, Count, DoFn, Filter, Flatten, GroupByKey, Mean, ParDo, Partition,
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
import scala.collection.mutable.{Buffer => MBuffer, Map => MMap}
import scala.reflect.ClassTag

object SCollection {
  def apply[T: ClassTag](p: PCollection[T])(implicit context: DataflowContext): SCollection[T] =
    new SCollectionImpl(p)

  def unionAll[T: ClassTag](scs: Iterable[SCollection[T]]): SCollection[T] = {
    val o = PCollectionList.of(scs.map(_.internal).asJava).apply(Flatten.pCollections().withName(CallSites.getCurrent))
    new SCollectionImpl(o)(scs.head.context, scs.head.ct)
  }
}

sealed trait SCollection[T] extends PCollectionWrapper[T] {

  import TupleFunctions._

  /* Delegations */

  def name: String = internal.getName

  def setCoder(coder: Coder[T]): SCollection[T] = SCollection(internal.setCoder(coder))

  def setName(name: String): SCollection[T] = SCollection(internal.setName(name))

  /* Collection operations */

  def ++(that: SCollection[T]): SCollection[T] = this.union(that)

  def union(that: SCollection[T]): SCollection[T] = {
    val o = PCollectionList
      .of(internal).and(that.internal)
      .apply(Flatten.pCollections().withName(CallSites.getCurrent))
    SCollection(o)
  }

  def intersection(that: SCollection[T]): SCollection[T] =
    this.map((_, 1)).coGroup(that.map((_, 1))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.nonEmpty) Seq(t._1) else Seq.empty
    }

  def partition(numPartitions: Int, f: T => Int): Seq[SCollection[T]] =
    this.applyInternal(Partition.of[T](numPartitions, Functions.partitionFn[T](numPartitions, f)))
      .getAll.asScala.map(p => SCollection(p))

  /* Transformations */

  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): SCollection[U] =
    this.apply(Combine.globally(Functions.aggregateFn(zeroValue)(seqOp, combOp)))

  // Algebird approach, more powerful and better optimized in some cases
  def aggregate[A: ClassTag, U: ClassTag](aggregator: Aggregator[T, A, U]): SCollection[U] =
    this.map(aggregator.prepare).sum()(aggregator.semigroup).map(aggregator.present)

  def combine[C: ClassTag](createCombiner: T => C)
                          (mergeValue: (C, T) => C)
                          (mergeCombiners: (C, C) => C): SCollection[C] =
    this.apply(Combine.globally(Functions.combineFn(createCombiner, mergeValue, mergeCombiners)))

  def count(): SCollection[Long] = this.apply(Count.globally[T]()).asInstanceOf[SCollection[Long]]

  def countApproxDistinct(sampleSize: Int): SCollection[Long] =
    this.apply(ApproximateUnique.globally[T](sampleSize)).asInstanceOf[SCollection[Long]]

  def countApproxDistinct(maximumEstimationError: Double = 0.02): SCollection[Long] =
    this.apply(ApproximateUnique.globally[T](maximumEstimationError)).asInstanceOf[SCollection[Long]]

  def countByValue(): SCollection[(T, Long)] =
    this.apply(Count.perElement[T]()).map(kvToTuple).asInstanceOf[SCollection[(T, Long)]]

  def distinct(): SCollection[T] = this.apply(RemoveDuplicates.create[T]())

  def filter(f: T => Boolean): SCollection[T] =
    this.apply(Filter.by(Functions.serializableFn(f.asInstanceOf[T => JBoolean])))

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): SCollection[U] = this.parDo(Functions.flatMapFn(f))

  def fold(zeroValue: T)(op: (T, T) => T): SCollection[T] =
    this.apply(Combine.globally(Functions.aggregateFn(zeroValue)(op, op)))

  // Algebird approach, more powerful and better optimized in some cases
  def fold(implicit mon: Monoid[T]): SCollection[T] = this.apply(Combine.globally(Functions.reduceFn(mon)))

  def groupBy[K: ClassTag](f: T => K): SCollection[(K, Iterable[T])] =
    this
      .apply(WithKeys.of(Functions.serializableFn(f))).setCoder(this.getKvCoder[K, T])
      .apply(GroupByKey.create[K, T]()).map(kvIterableToTuple)

  // Scala lambda is simpler than transforms.WithKeys
  def keyBy[K: ClassTag](f: T => K): SCollection[(K, T)] = this.map(v => (f(v), v))

  def map[U: ClassTag](f: T => U): SCollection[U] = this.parDo(Functions.mapFn(f))

  // Scala lambda is simpler and more powerful than transforms.Max
  def max()(implicit ord: Ordering[T]): SCollection[T] = this.reduce(ord.max)

  def mean()(implicit ev: Numeric[T]): SCollection[Double] = {
    val o = this
      .map(ev.toDouble).asInstanceOf[SCollection[JDouble]]
      .applyInternal(Mean.globally()).asInstanceOf[PCollection[Double]]
    SCollection(o)
  }

  // Scala lambda is simpler and more powerful than transforms.Min
  def min()(implicit ord: Ordering[T]): SCollection[T] = this.reduce(ord.min)

  def quantilesApprox(numQuantiles: Int)(implicit ord: Ordering[T]): SCollection[Iterable[T]] =
    this.apply(ApproximateQuantiles.globally(numQuantiles, ord)).map(_.asInstanceOf[JIterable[T]].asScala)

  def reduce(op: (T, T) => T): SCollection[T] = this.apply(Combine.globally(Functions.reduceFn(op)))

  def sample(sampleSize: Int): SCollection[Iterable[T]] =
    this.apply(Sample.fixedSizeGlobally(sampleSize)).map(_.asScala)

  def subtract(that: SCollection[T]): SCollection[T] =
    this.map((_, 1)).coGroup(that.map((_, 1))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.isEmpty) Seq(t._1) else Seq.empty
    }

  // Algebird approach, more powerful and better optimized in some cases
  def sum()(implicit sg: Semigroup[T]): SCollection[T] = this.apply(Combine.globally(Functions.reduceFn(sg)))

  def take(num: Long): SCollection[T] = this.apply(Sample.any(num))

  def top(num: Int)(implicit ord: Ordering[T]): SCollection[Iterable[T]] =
    this.apply(Top.of(num, ord)).map(_.asInstanceOf[JIterable[T]].asScala)

  /* Hash operations */

  def hashLookup[V: ClassTag](that: SCollection[(T, V)]): SCollection[(T, Seq[V])] =
    this.withKVSideInput(that).map((t, m) => (t, m.getOrElse(t, Seq()))).toSCollection

  /* Accumulator operations */

  def withAccumulator: SCollectionWithAccumulator[T] = new SCollectionWithAccumulator[T](internal)

  /* Side input operations */

  def withSingletonSideInput[U](that: SCollection[U]): SCollectionWithSideInput[T, U] = {
    val view = that.applyInternal(View.asSingleton())
    new SCollectionWithSideInputImpl[T, U, U](internal, view, identity)
  }

  def withIterableSideInput[U](that: SCollection[U]): SCollectionWithSideInput[T, Iterable[U]] = {
    val view = that.applyInternal(View.asIterable())
    type S = JIterable[U]
    new SCollectionWithSideInputImpl[T, Iterable[U], S](internal, view, _.asScala)
  }

  def withKVSideInput[K: ClassTag, V: ClassTag](that: SCollection[(K, V)])
      : SCollectionWithSideInput[T, Map[K, Seq[V]]] = {
    val view = that.toKV.applyInternal(View.asIterable())
    val sideFn = (s: JIterable[KV[K, V]]) => {
      val sideMap: MMap[K, MBuffer[V]] = MMap.empty
      s.asScala.foreach(kv => sideMap.getOrElseUpdate(kv.getKey, MBuffer.empty).append(kv.getValue))
      sideMap.mapValues(_.toSeq).toMap
    }
    new SCollectionWithSideInputImpl(internal, view, sideFn)
  }

  /* Side output operations */

  def mapWithSideOutput[U: ClassTag, S: ClassTag](f: T => (U, S)): (SCollection[U], SCollection[S]) = {
    val mainTag = new TupleTag[U]()
    val sideTag = new TupleTag[S]()

    val tuple = this.applyInternal(ParDo
      .withOutputTags(mainTag, TupleTagList.of(sideTag))
      .of(FunctionsWithSideOutput.mapFn(f, sideTag)))

    val o = tuple.get(mainTag).setCoder(this.getCoder[U])
    val s = tuple.get(sideTag).setCoder(this.getCoder[S])
    (SCollection(o), SCollection(s))
  }

  def flatMapWithSideOutput[U: ClassTag, S: ClassTag](f: T => (TraversableOnce[U], TraversableOnce[S]))
      : (SCollection[U], SCollection[S]) = {
    val mainTag = new TupleTag[U]()
    val sideTag = new TupleTag[S]()

    val tuple = this.applyInternal(ParDo
      .withOutputTags(mainTag, TupleTagList.of(sideTag))
      .of(FunctionsWithSideOutput.flatMapFn(f, sideTag)))

    val o = tuple.get(mainTag).setCoder(this.getCoder[U])
    val s = tuple.get(sideTag).setCoder(this.getCoder[S])
    (SCollection(o), SCollection(s))
  }

  /* Windowing operations */

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

  /* Write operations */

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

  def saveAsBigQuery(tableSpec: String, schema: TableSchema = null,
                     createDisposition: CreateDisposition = null,
                     writeDisposition: WriteDisposition = null)
                    (implicit ev: T <:< TableRow): Unit =
    saveAsBigQuery(GBigQueryIO.parseTableSpec(tableSpec), schema, createDisposition, writeDisposition)

  def saveAsDatastore(datasetId: String)(implicit ev: T <:< Entity): Unit =
    if (context.isTest) {
      context.testOut(DatastoreIO(datasetId))(internal.asInstanceOf[PCollection[Entity]])
    } else {
      this.asInstanceOf[SCollection[Entity]].applyInternal(GDatastoreIO.writeTo(datasetId))
    }

  def saveAsPubsub(topic: String)(implicit ev: T <:< String): Unit =
    if (context.isTest) {
      context.testOut(PubsubIO(topic))(internal.asInstanceOf[PCollection[String]])
    } else {
      this.asInstanceOf[SCollection[String]].applyInternal(GPubsubIO.Write.topic(topic))
    }

  def saveAsTableRowJsonFile(path: String, numShards: Int = 0)(implicit ev: T <:< TableRow): Unit =
    if (context.isTest) {
      context.testOut(BigQueryIO(path))(internal.asInstanceOf[PCollection[TableRow]])
    } else {
      this.asInstanceOf[SCollection[TableRow]].applyInternal(tableRowJsonOut(path, numShards))
    }

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
