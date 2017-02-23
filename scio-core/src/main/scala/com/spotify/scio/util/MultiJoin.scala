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
// scalastyle:off method.length
// scalastyle:off number.of.methods
// scalastyle:off parameter.number

package com.spotify.scio.util

import com.google.common.collect.Lists
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.TupleTag

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait MultiJoin extends Serializable {

  protected def tfName: String = CallSites.getCurrent

  def toOptions[T](xs: Iterator[T]): Iterator[Option[T]] = if (xs.isEmpty) Iterator(None) else xs.map(Option(_))

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Iterable[A], Iterable[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C]))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] = {
    val (tagA, tagB, tagC, tagD, tagE) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .and(tagK, k.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, B))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, B, C))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, B, C, D))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, B, C, D, E))] = {
    val (tagA, tagB, tagC, tagD, tagE) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        e <- result.getAll(tagE).asScala.iterator
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, B, C, D, E, F))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        f <- result.getAll(tagF).asScala.iterator
        e <- result.getAll(tagE).asScala.iterator
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, B, C, D, E, F, G))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        g <- result.getAll(tagG).asScala.iterator
        f <- result.getAll(tagF).asScala.iterator
        e <- result.getAll(tagE).asScala.iterator
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, B, C, D, E, F, G, H))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        h <- result.getAll(tagH).asScala.iterator
        g <- result.getAll(tagG).asScala.iterator
        f <- result.getAll(tagF).asScala.iterator
        e <- result.getAll(tagE).asScala.iterator
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        i <- result.getAll(tagI).asScala.iterator
        h <- result.getAll(tagH).asScala.iterator
        g <- result.getAll(tagG).asScala.iterator
        f <- result.getAll(tagF).asScala.iterator
        e <- result.getAll(tagE).asScala.iterator
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h, i))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        j <- result.getAll(tagJ).asScala.iterator
        i <- result.getAll(tagI).asScala.iterator
        h <- result.getAll(tagH).asScala.iterator
        g <- result.getAll(tagG).asScala.iterator
        f <- result.getAll(tagF).asScala.iterator
        e <- result.getAll(tagE).asScala.iterator
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .and(tagK, k.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        k <- result.getAll(tagK).asScala.iterator
        j <- result.getAll(tagJ).asScala.iterator
        i <- result.getAll(tagI).asScala.iterator
        h <- result.getAll(tagH).asScala.iterator
        g <- result.getAll(tagG).asScala.iterator
        f <- result.getAll(tagF).asScala.iterator
        e <- result.getAll(tagE).asScala.iterator
        d <- result.getAll(tagD).asScala.iterator
        c <- result.getAll(tagC).asScala.iterator
        b <- result.getAll(tagB).asScala.iterator
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, Option[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, Option[B], Option[C]))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D]))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E]))] = {
    val (tagA, tagB, tagC, tagD, tagE) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        i <- toOptions(result.getAll(tagI).asScala.iterator)
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h, i))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        j <- toOptions(result.getAll(tagJ).asScala.iterator)
        i <- toOptions(result.getAll(tagI).asScala.iterator)
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j))
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .and(tagK, k.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        k <- toOptions(result.getAll(tagK).asScala.iterator)
        j <- toOptions(result.getAll(tagJ).asScala.iterator)
        i <- toOptions(result.getAll(tagI).asScala.iterator)
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- result.getAll(tagA).asScala.iterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Option[A], Option[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Option[A], Option[B], Option[C]))] = {
    val (tagA, tagB, tagC) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D]))] = {
    val (tagA, tagB, tagC, tagD) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E]))] = {
    val (tagA, tagB, tagC, tagD, tagE) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d, e))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d, e, f))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d, e, f, g))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d, e, f, g, h))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        i <- toOptions(result.getAll(tagI).asScala.iterator)
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d, e, f, g, h, i))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        j <- toOptions(result.getAll(tagJ).asScala.iterator)
        i <- toOptions(result.getAll(tagI).asScala.iterator)
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j))
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .and(tagF, f.toKV.internal)
      .and(tagG, g.toKV.internal)
      .and(tagH, h.toKV.internal)
      .and(tagI, i.toKV.internal)
      .and(tagJ, j.toKV.internal)
      .and(tagK, k.toKV.internal)
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        k <- toOptions(result.getAll(tagK).asScala.iterator)
        j <- toOptions(result.getAll(tagJ).asScala.iterator)
        i <- toOptions(result.getAll(tagI).asScala.iterator)
        h <- toOptions(result.getAll(tagH).asScala.iterator)
        g <- toOptions(result.getAll(tagG).asScala.iterator)
        f <- toOptions(result.getAll(tagF).asScala.iterator)
        e <- toOptions(result.getAll(tagE).asScala.iterator)
        d <- toOptions(result.getAll(tagD).asScala.iterator)
        c <- toOptions(result.getAll(tagC).asScala.iterator)
        b <- toOptions(result.getAll(tagB).asScala.iterator)
        a <- toOptions(result.getAll(tagA).asScala.iterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k))
    }
  }

}


object MultiJoin extends MultiJoin {
  def withName(name: String): MultiJoin = new NamedMultiJoin(name)
}

private class NamedMultiJoin(val name: String) extends MultiJoin {
  override def tfName: String = name
}

// scalastyle:on cyclomatic.complexity
// scalastyle:on file.size.limit
// scalastyle:on line.size.limit
// scalastyle:on method.length
// scalastyle:on number.of.methods
// scalastyle:on parameter.number
