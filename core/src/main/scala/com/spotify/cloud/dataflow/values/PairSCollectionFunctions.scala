package com.spotify.cloud.dataflow.values

import java.lang.{Long => JLong}

import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import com.google.cloud.dataflow.sdk.values.{PCollection, KV, TupleTag}
import com.spotify.cloud.dataflow.{Implicits, DataflowContext}
import com.spotify.cloud.dataflow.util._
import com.twitter.algebird.{Aggregator, Monoid, Semigroup}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class PairSCollectionFunctions[K, V](val self: SCollection[(K, V)])
                                    (implicit ctKey: ClassTag[K], ctValue: ClassTag[V]) extends Implicits {

  import TupleFunctions._

  implicit def context: DataflowContext = self.context

  private def toKvTransform = ParDo.of(Functions.mapFn[(K, V), KV[K, V]](kv => KV.of(kv._1, kv._2)))

  private[values] def toKV: SCollection[KV[K, V]] = {
    val o = self.applyInternal(toKvTransform).setCoder(self.getKvCoder[K, V])
    SCollection(o)
  }

  private[values] def applyKv[U: ClassTag](t: PTransform[_ >: PCollection[KV[K, V]], PCollection[U]])
  : SCollection[U] = {
    val o = self.applyInternal(new PTransform[PCollection[(K, V)], PCollection[U]]() {
      override def apply(input: PCollection[(K, V)]): PCollection[U] =
        input
          .apply(toKvTransform.withName("TupleToKv"))
          .setCoder(self.getKvCoder[K, V])
          .apply(t)
          .setCoder(self.getCoder[U])
    }.withName(CallSites.getCurrent))
    SCollection(o)
  }

  private def applyPerKey[UI: ClassTag, UO: ClassTag](t: PTransform[PCollection[KV[K, V]], PCollection[KV[K, UI]]],
                                                      f: KV[K, UI] => (K, UO))
  : SCollection[(K, UO)] = {
    val o = self.applyInternal(new PTransform[PCollection[(K, V)], PCollection[(K, UO)]]() {
      override def apply(input: PCollection[(K, V)]): PCollection[(K, UO)] =
        input
          .apply(toKvTransform.withName("TupleToKv"))
          .setCoder(self.getKvCoder[K, V])
          .apply(t)
          .apply(ParDo.of(Functions.mapFn[KV[K, UI], (K, UO)](f)).withName("KvToTuple"))
          .setCoder(self.getCoder[(K, UO)])
    }.withName(CallSites.getCurrent))
    SCollection(o)
  }

  /* CoGroups */

  def coGroup[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Iterable[V], Iterable[W]))] = {
    val (tagV, tagW) = (new TupleTag[V](), new TupleTag[W]())
    val keyed = KeyedPCollectionTuple
      .of(tagV, this.toKV.internal)
      .and(tagW, that.toKV.internal)
      .apply(CoGroupByKey.create().withName(CallSites.getCurrent))

    SCollection(keyed).map { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      val (v, w) = (r.getAll(tagV).asScala, r.getAll(tagW).asScala)
      (k, (v, w))
    }
  }

  def coGroup[W1: ClassTag, W2: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = {
    val (tagV, tagW1, tagW2) = (new TupleTag[V](), new TupleTag[W1](), new TupleTag[W2]())
    val keyed = KeyedPCollectionTuple
      .of(tagV, this.toKV.internal)
      .and(tagW1, that1.toKV.internal)
      .and(tagW2, that2.toKV.internal)
      .apply(CoGroupByKey.create().withName(CallSites.getCurrent))

    SCollection(keyed).map { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      val (v, w1, w2) = (r.getAll(tagV).asScala, r.getAll(tagW1).asScala, r.getAll(tagW2).asScala)
      (k, (v, w1, w2))
    }
  }

  def coGroup[W1: ClassTag, W2: ClassTag, W3: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = {
    val (tagV, tagW1, tagW2, tagW3) = (new TupleTag[V](), new TupleTag[W1](), new TupleTag[W2](), new TupleTag[W3]())
    val keyed = KeyedPCollectionTuple
      .of(tagV, this.toKV.internal)
      .and(tagW1, that1.toKV.internal)
      .and(tagW2, that2.toKV.internal)
      .and(tagW3, that3.toKV.internal)
      .apply(CoGroupByKey.create().withName(CallSites.getCurrent))

    SCollection(keyed).map { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      val v = r.getAll(tagV).asScala
      val (w1, w2, w3) = (r.getAll(tagW1).asScala, r.getAll(tagW2).asScala, r.getAll(tagW3).asScala)
      (k, (v, w1, w2, w3))
    }
  }

  def groupWith[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Iterable[V], Iterable[W]))] =
    this.coGroup(that)

  def groupWith[W1: ClassTag, W2: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    this.coGroup(that1, that2)

  def groupWith[W1: ClassTag, W2: ClassTag, W3: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    this.coGroup(that1, that2, that3)

  /* Joins */

  def fullOuterJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Option[V], Option[W]))] =
    this.coGroup(that).flatMapValues { t =>
      val lhs = if (t._1.isEmpty) Iterable(null.asInstanceOf[V]) else t._1
      val rhs = if (t._2.isEmpty) Iterable(null.asInstanceOf[W]) else t._2
      lhs.flatMap(v => rhs.map(w => (Option(v), Option(w))))
    }

  def join[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, W))] =
    this.coGroup(that).flatMapValues { t =>
      val (lhs, rhs) = t
      lhs.flatMap(v => rhs.map(w => (v, w)))
    }

  def leftOuterJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, Option[W]))] =
    this.coGroup(that).flatMapValues { t =>
      val lhs = t._1
      val rhs = if (t._2.isEmpty) Iterable(null.asInstanceOf[W]) else t._2
      lhs.flatMap(v => rhs.map(w => (v, Option(w))))
    }

  def rightOuterJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Option[V], W))] =
    this.coGroup(that).flatMapValues { t =>
      val lhs = if (t._1.isEmpty) Iterable(null.asInstanceOf[V]) else t._1
      val rhs = t._2
      lhs.flatMap(v => rhs.map(w => (Option(v), w)))
    }

  /* Transformations */

  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): SCollection[(K, U)] =
    this.applyPerKey(Combine.perKey(Functions.aggregateFn(zeroValue)(seqOp, combOp)), kvToTuple[K, U])

  // Algebird approach, more powerful and better optimized in some cases
  def aggregateByKey[A: ClassTag, U: ClassTag](aggregator: Aggregator[V, A, U]): SCollection[(K, U)] =
    this.mapValues(aggregator.prepare).sumByKey()(aggregator.semigroup).mapValues(aggregator.present)

  def approxQuantilesByKey(numQuantiles: Int)(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(ApproximateQuantiles.perKey(numQuantiles, ord), kvListToTuple[K, V])

  def combineByKey[C: ClassTag](createCombiner: V => C)
                               (mergeValue: (C, V) => C)
                               (mergeCombiners: (C, C) => C): SCollection[(K, C)] =
    this.applyPerKey(Combine.perKey(Functions.combineFn(createCombiner, mergeValue, mergeCombiners)), kvToTuple[K, C])

  def countApproxDistinctByKey(sampleSize: Int): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](sampleSize), kvToTuple[K, JLong])
      .asInstanceOf[SCollection[(K, Long)]]

  def countApproxDistinctByKey(maximumEstimationError: Double = 0.02): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](maximumEstimationError), kvToTuple[K, JLong])
      .asInstanceOf[SCollection[(K, Long)]]

  def countByKey(): SCollection[(K, Long)] = this.keys.countByValue()

  def flatMapValues[U: ClassTag](f: V => TraversableOnce[U]): SCollection[(K, U)] =
    self.flatMap(kv => f(kv._2).map(v => (kv._1, v)))

  def foldByKey(zeroValue: V)(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.aggregateFn(zeroValue)(op, op)), kvToTuple[K, V])

  // Algebird approach, more powerful and better optimized in some cases
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(mon)), kvToTuple[K, V])

  def groupByKey(): SCollection[(K, Iterable[V])] =
    this.applyPerKey(GroupByKey.create[K, V](), kvIterableToTuple[K, V])

  def keys: SCollection[K] = this.applyKv(Keys.create[K]())

  def mapValues[U: ClassTag](f: V => U): SCollection[(K, U)] = self.map(kv => (kv._1, f(kv._2)))

  // Scala lambda is simpler and more powerful than transforms.Max
  def maxByKey()(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.max)

  // Scala lambda is simpler and more powerful than transforms.Min
  def minByKey()(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.min)

  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(op)), kvToTuple[K, V])

  def sampleByKey(sampleSize: Int): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Sample.fixedSizePerKey[K, V](sampleSize), kvIterableToTuple[K, V])

  def subtractByKey[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, V)] =
    this.coGroup(that).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.isEmpty) t._2._1.map((t._1, _)) else  Seq.empty
    }

  // Algebird approach, more powerful and better optimized in some cases
  def sumByKey()(implicit sg: Semigroup[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(sg)), kvToTuple[K, V])

  // Scala lambda is simpler than transforms.KvSwap
  def swap: SCollection[(V, K)] = self.map(kv => (kv._2, kv._1))

  def topByKey(num: Int)(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Top.perKey[K, V, Ordering[V]](num, ord), kvListToTuple[K, V])

  def values: SCollection[V] = this.applyKv(Values.create[V]())

  /* Hash operations */

  def hashJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, W))] = {
    val side = that.asMapSideInput
    self.withSideInputs(side).flatMap[(K, (V, W))] { (kv, s) =>
      s(side).getOrElse(kv._1, Iterable()).toSeq.map(w => (kv._1, (kv._2, w)))
    }.toSCollection
  }

  def hashLeftJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, Option[W]))] = {
    val side = that.asMapSideInput
    self.withSideInputs(side).flatMap[(K, (V, Option[W]))] { (kv, s) =>
      val (k, v) = kv
      val m = s(side)
      if (m.contains(k)) m(k).map(w => (k, (v, Some(w)))) else Seq((k, (v, None)))
    }.toSCollection
  }

  /* Side input operations */

  def asMapSideInput: SideInput[Map[K, Iterable[V]]] = new MapSideInput[K, V](self.toKV.applyInternal(View.asMap()))

}
