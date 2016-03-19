
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

// generated with multijoin.py

// scalastyle:off cyclomatic.complexity
// scalastyle:off file.size.limit
// scalastyle:off line.size.limit
// scalastyle:off number.of.methods
// scalastyle:off parameter.number

package com.spotify.scio.util

import com.google.cloud.dataflow.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import com.google.cloud.dataflow.sdk.values.TupleTag
import com.spotify.scio.values.SCollection

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object MultiJoin {

  def toOptions[T](xs: Iterable[T]): Iterable[Option[T]] = if (xs.isEmpty) Iterable(None) else xs.map(Option(_))

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Iterable[A], Iterable[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C]))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, B))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- r.getAll(tagA).asScala
        b <- r.getAll(tagB).asScala
      } yield (k, (a, b))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, B, C))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- r.getAll(tagA).asScala
        b <- r.getAll(tagB).asScala
        c <- r.getAll(tagC).asScala
      } yield (k, (a, b, c))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, B, C, D))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- r.getAll(tagA).asScala
        b <- r.getAll(tagB).asScala
        c <- r.getAll(tagC).asScala
        d <- r.getAll(tagD).asScala
      } yield (k, (a, b, c, d))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, Option[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- r.getAll(tagA).asScala
        b <- toOptions(r.getAll(tagB).asScala)
      } yield (k, (a, b))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, Option[B], Option[C]))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- r.getAll(tagA).asScala
        b <- toOptions(r.getAll(tagB).asScala)
        c <- toOptions(r.getAll(tagC).asScala)
      } yield (k, (a, b, c))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D]))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- r.getAll(tagA).asScala
        b <- toOptions(r.getAll(tagB).asScala)
        c <- toOptions(r.getAll(tagC).asScala)
        d <- toOptions(r.getAll(tagD).asScala)
      } yield (k, (a, b, c, d))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Option[A], Option[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- toOptions(r.getAll(tagA).asScala)
        b <- toOptions(r.getAll(tagB).asScala)
      } yield (k, (a, b))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Option[A], Option[B], Option[C]))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- toOptions(r.getAll(tagA).asScala)
        b <- toOptions(r.getAll(tagB).asScala)
        c <- toOptions(r.getAll(tagC).asScala)
      } yield (k, (a, b, c))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D]))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      for {
        a <- toOptions(r.getAll(tagA).asScala)
        b <- toOptions(r.getAll(tagB).asScala)
        c <- toOptions(r.getAll(tagC).asScala)
        d <- toOptions(r.getAll(tagD).asScala)
      } yield (k, (a, b, c, d))
    }
  }

}

// scalastyle:on cyclomatic.complexity
// scalastyle:on file.size.limit
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
// scalastyle:on parameter.number
