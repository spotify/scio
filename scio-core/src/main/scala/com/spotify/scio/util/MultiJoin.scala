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

import com.google.cloud.dataflow.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import com.google.cloud.dataflow.sdk.values.TupleTag
import com.spotify.scio.values.SCollection

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object MultiJoin {

  def toOptions[T](xs: Iterator[T]): Iterator[Option[T]] = if (xs.isEmpty) Iterator(None) else xs.map(Option(_))

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Iterable[A], Iterable[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L]())
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
      .and(tagL, l.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala, result.getAll(tagT).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala, result.getAll(tagT).asScala, result.getAll(tagU).asScala))
    }
  }

  def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU, tagV) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U](), new TupleTag[V]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .and(tagV, v.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala, result.getAll(tagT).asScala, result.getAll(tagU).asScala, result.getAll(tagV).asScala))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, B))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
      } yield (key, (a, b))
      iterator.toIterable
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
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
      } yield (key, (a, b, c))
      iterator.toIterable
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
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
      } yield (key, (a, b, c, d))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
      } yield (key, (a, b, c, d, e))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
      } yield (key, (a, b, c, d, e, f))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L]())
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
      .and(tagL, l.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
        p <- result.getAll(tagP).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
        p <- result.getAll(tagP).asScala.toIterator
        q <- result.getAll(tagQ).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
        p <- result.getAll(tagP).asScala.toIterator
        q <- result.getAll(tagQ).asScala.toIterator
        r <- result.getAll(tagR).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
        p <- result.getAll(tagP).asScala.toIterator
        q <- result.getAll(tagQ).asScala.toIterator
        r <- result.getAll(tagR).asScala.toIterator
        s <- result.getAll(tagS).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
        p <- result.getAll(tagP).asScala.toIterator
        q <- result.getAll(tagQ).asScala.toIterator
        r <- result.getAll(tagR).asScala.toIterator
        s <- result.getAll(tagS).asScala.toIterator
        t <- result.getAll(tagT).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
        p <- result.getAll(tagP).asScala.toIterator
        q <- result.getAll(tagQ).asScala.toIterator
        r <- result.getAll(tagR).asScala.toIterator
        s <- result.getAll(tagS).asScala.toIterator
        t <- result.getAll(tagT).asScala.toIterator
        u <- result.getAll(tagU).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
      iterator.toIterable
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU, tagV) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U](), new TupleTag[V]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .and(tagV, v.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- result.getAll(tagB).asScala.toIterator
        c <- result.getAll(tagC).asScala.toIterator
        d <- result.getAll(tagD).asScala.toIterator
        e <- result.getAll(tagE).asScala.toIterator
        f <- result.getAll(tagF).asScala.toIterator
        g <- result.getAll(tagG).asScala.toIterator
        h <- result.getAll(tagH).asScala.toIterator
        i <- result.getAll(tagI).asScala.toIterator
        j <- result.getAll(tagJ).asScala.toIterator
        k <- result.getAll(tagK).asScala.toIterator
        l <- result.getAll(tagL).asScala.toIterator
        m <- result.getAll(tagM).asScala.toIterator
        n <- result.getAll(tagN).asScala.toIterator
        o <- result.getAll(tagO).asScala.toIterator
        p <- result.getAll(tagP).asScala.toIterator
        q <- result.getAll(tagQ).asScala.toIterator
        r <- result.getAll(tagR).asScala.toIterator
        s <- result.getAll(tagS).asScala.toIterator
        t <- result.getAll(tagT).asScala.toIterator
        u <- result.getAll(tagU).asScala.toIterator
        v <- result.getAll(tagV).asScala.toIterator
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, Option[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
      } yield (key, (a, b))
      iterator.toIterable
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
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
      } yield (key, (a, b, c))
      iterator.toIterable
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
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
      } yield (key, (a, b, c, d))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
      } yield (key, (a, b, c, d, e))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L]())
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
      .and(tagL, l.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
        t <- toOptions(result.getAll(tagT).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
        t <- toOptions(result.getAll(tagT).asScala.toIterator)
        u <- toOptions(result.getAll(tagU).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
      iterator.toIterable
    }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU, tagV) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U](), new TupleTag[V]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .and(tagV, v.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- result.getAll(tagA).asScala.toIterator
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
        t <- toOptions(result.getAll(tagT).asScala.toIterator)
        u <- toOptions(result.getAll(tagU).asScala.toIterator)
        v <- toOptions(result.getAll(tagV).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Option[A], Option[B]))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
      } yield (key, (a, b))
      iterator.toIterable
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
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
      } yield (key, (a, b, c))
      iterator.toIterable
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
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
      } yield (key, (a, b, c, d))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
      } yield (key, (a, b, c, d, e))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j))
      iterator.toIterable
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
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L]())
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
      .and(tagL, l.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
        t <- toOptions(result.getAll(tagT).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
        t <- toOptions(result.getAll(tagT).asScala.toIterator)
        u <- toOptions(result.getAll(tagU).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
      iterator.toIterable
    }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
    val (tagA, tagB, tagC, tagD, tagE, tagF, tagG, tagH, tagI, tagJ, tagK, tagL, tagM, tagN, tagO, tagP, tagQ, tagR, tagS, tagT, tagU, tagV) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E](), new TupleTag[F](), new TupleTag[G](), new TupleTag[H](), new TupleTag[I](), new TupleTag[J](), new TupleTag[K](), new TupleTag[L](), new TupleTag[M](), new TupleTag[N](), new TupleTag[O](), new TupleTag[P](), new TupleTag[Q](), new TupleTag[R](), new TupleTag[S](), new TupleTag[T](), new TupleTag[U](), new TupleTag[V]())
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
      .and(tagL, l.toKV.internal)
      .and(tagM, m.toKV.internal)
      .and(tagN, n.toKV.internal)
      .and(tagO, o.toKV.internal)
      .and(tagP, p.toKV.internal)
      .and(tagQ, q.toKV.internal)
      .and(tagR, r.toKV.internal)
      .and(tagS, s.toKV.internal)
      .and(tagT, t.toKV.internal)
      .and(tagU, u.toKV.internal)
      .and(tagV, v.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      val iterator = for {
        a <- toOptions(result.getAll(tagA).asScala.toIterator)
        b <- toOptions(result.getAll(tagB).asScala.toIterator)
        c <- toOptions(result.getAll(tagC).asScala.toIterator)
        d <- toOptions(result.getAll(tagD).asScala.toIterator)
        e <- toOptions(result.getAll(tagE).asScala.toIterator)
        f <- toOptions(result.getAll(tagF).asScala.toIterator)
        g <- toOptions(result.getAll(tagG).asScala.toIterator)
        h <- toOptions(result.getAll(tagH).asScala.toIterator)
        i <- toOptions(result.getAll(tagI).asScala.toIterator)
        j <- toOptions(result.getAll(tagJ).asScala.toIterator)
        k <- toOptions(result.getAll(tagK).asScala.toIterator)
        l <- toOptions(result.getAll(tagL).asScala.toIterator)
        m <- toOptions(result.getAll(tagM).asScala.toIterator)
        n <- toOptions(result.getAll(tagN).asScala.toIterator)
        o <- toOptions(result.getAll(tagO).asScala.toIterator)
        p <- toOptions(result.getAll(tagP).asScala.toIterator)
        q <- toOptions(result.getAll(tagQ).asScala.toIterator)
        r <- toOptions(result.getAll(tagR).asScala.toIterator)
        s <- toOptions(result.getAll(tagS).asScala.toIterator)
        t <- toOptions(result.getAll(tagT).asScala.toIterator)
        u <- toOptions(result.getAll(tagU).asScala.toIterator)
        v <- toOptions(result.getAll(tagV).asScala.toIterator)
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
      iterator.toIterable
    }
  }

}

// scalastyle:on cyclomatic.complexity
// scalastyle:on file.size.limit
// scalastyle:on line.size.limit
// scalastyle:on method.length
// scalastyle:on number.of.methods
// scalastyle:on parameter.number
