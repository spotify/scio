
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

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Iterable[A], Iterable[B]))] = {
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

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C]))] = {
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

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
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

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] = {
    val (tagA, tagB, tagC, tagD, tagE) = (new TupleTag[A](), new TupleTag[B](), new TupleTag[C](), new TupleTag[D](), new TupleTag[E]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .and(tagC, c.toKV.internal)
      .and(tagD, d.toKV.internal)
      .and(tagE, e.toKV.internal)
      .apply(CallSites.getCurrent, CoGroupByKey.create())
    a.context.wrap(keyed).map { kv =>
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala, r.getAll(tagP).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala, r.getAll(tagP).asScala, r.getAll(tagQ).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala, r.getAll(tagP).asScala, r.getAll(tagQ).asScala, r.getAll(tagR).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala, r.getAll(tagP).asScala, r.getAll(tagQ).asScala, r.getAll(tagR).asScala, r.getAll(tagS).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala, r.getAll(tagP).asScala, r.getAll(tagQ).asScala, r.getAll(tagR).asScala, r.getAll(tagS).asScala, r.getAll(tagT).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala, r.getAll(tagP).asScala, r.getAll(tagQ).asScala, r.getAll(tagR).asScala, r.getAll(tagS).asScala, r.getAll(tagT).asScala, r.getAll(tagU).asScala))
    }
  }

  def coGroup[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V]))] = {
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
      val (k, r) = (kv.getKey, kv.getValue)
      (k, (r.getAll(tagA).asScala, r.getAll(tagB).asScala, r.getAll(tagC).asScala, r.getAll(tagD).asScala, r.getAll(tagE).asScala, r.getAll(tagF).asScala, r.getAll(tagG).asScala, r.getAll(tagH).asScala, r.getAll(tagI).asScala, r.getAll(tagJ).asScala, r.getAll(tagK).asScala, r.getAll(tagL).asScala, r.getAll(tagM).asScala, r.getAll(tagN).asScala, r.getAll(tagO).asScala, r.getAll(tagP).asScala, r.getAll(tagQ).asScala, r.getAll(tagR).asScala, r.getAll(tagS).asScala, r.getAll(tagT).asScala, r.getAll(tagU).asScala, r.getAll(tagV).asScala))
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, B))] = {
    coGroup(a, b)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
        } yield (kv._1, (a, b))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, B, C))] = {
    coGroup(a, b, c)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
        } yield (kv._1, (a, b, c))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, B, C, D))] = {
    coGroup(a, b, c, d)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
        } yield (kv._1, (a, b, c, d))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, B, C, D, E))] = {
    coGroup(a, b, c, d, e)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
        } yield (kv._1, (a, b, c, d, e))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, B, C, D, E, F))] = {
    coGroup(a, b, c, d, e, f)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
        } yield (kv._1, (a, b, c, d, e, f))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, B, C, D, E, F, G))] = {
    coGroup(a, b, c, d, e, f, g)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
        } yield (kv._1, (a, b, c, d, e, f, g))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, B, C, D, E, F, G, H))] = {
    coGroup(a, b, c, d, e, f, g, h)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
        } yield (kv._1, (a, b, c, d, e, f, g, h))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I))] = {
    coGroup(a, b, c, d, e, f, g, h, i)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
        } yield (kv._1, (a, b, c, d, e, f, g, h, i))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
          p <- kv._2._16
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
          p <- kv._2._16
          q <- kv._2._17
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
          p <- kv._2._16
          q <- kv._2._17
          r <- kv._2._18
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
          p <- kv._2._16
          q <- kv._2._17
          r <- kv._2._18
          s <- kv._2._19
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
          p <- kv._2._16
          q <- kv._2._17
          r <- kv._2._18
          s <- kv._2._19
          t <- kv._2._20
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
          p <- kv._2._16
          q <- kv._2._17
          r <- kv._2._18
          s <- kv._2._19
          t <- kv._2._20
          u <- kv._2._21
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
      }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- kv._2._2
          c <- kv._2._3
          d <- kv._2._4
          e <- kv._2._5
          f <- kv._2._6
          g <- kv._2._7
          h <- kv._2._8
          i <- kv._2._9
          j <- kv._2._10
          k <- kv._2._11
          l <- kv._2._12
          m <- kv._2._13
          n <- kv._2._14
          o <- kv._2._15
          p <- kv._2._16
          q <- kv._2._17
          r <- kv._2._18
          s <- kv._2._19
          t <- kv._2._20
          u <- kv._2._21
          v <- kv._2._22
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, Option[B]))] = {
    coGroup(a, b)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
        } yield (kv._1, (a, b))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, Option[B], Option[C]))] = {
    coGroup(a, b, c)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
        } yield (kv._1, (a, b, c))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D]))] = {
    coGroup(a, b, c, d)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
        } yield (kv._1, (a, b, c, d))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E]))] = {
    coGroup(a, b, c, d, e)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
        } yield (kv._1, (a, b, c, d, e))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
    coGroup(a, b, c, d, e, f)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
    coGroup(a, b, c, d, e, f, g)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
    coGroup(a, b, c, d, e, f, g, h)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
    coGroup(a, b, c, d, e, f, g, h, i)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
          t <- if (kv._2._20.isEmpty) Iterable(None) else kv._2._20.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
          t <- if (kv._2._20.isEmpty) Iterable(None) else kv._2._20.map(Option(_))
          u <- if (kv._2._21.isEmpty) Iterable(None) else kv._2._21.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
      }
  }

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      .flatMap { kv =>
        for {
          a <- kv._2._1
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
          t <- if (kv._2._20.isEmpty) Iterable(None) else kv._2._20.map(Option(_))
          u <- if (kv._2._21.isEmpty) Iterable(None) else kv._2._21.map(Option(_))
          v <- if (kv._2._22.isEmpty) Iterable(None) else kv._2._22.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Option[A], Option[B]))] = {
    coGroup(a, b)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
        } yield (kv._1, (a, b))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Option[A], Option[B], Option[C]))] = {
    coGroup(a, b, c)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
        } yield (kv._1, (a, b, c))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D]))] = {
    coGroup(a, b, c, d)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
        } yield (kv._1, (a, b, c, d))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E]))] = {
    coGroup(a, b, c, d, e)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
        } yield (kv._1, (a, b, c, d, e))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
    coGroup(a, b, c, d, e, f)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
    coGroup(a, b, c, d, e, f, g)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
    coGroup(a, b, c, d, e, f, g, h)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
    coGroup(a, b, c, d, e, f, g, h, i)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
          t <- if (kv._2._20.isEmpty) Iterable(None) else kv._2._20.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
          t <- if (kv._2._20.isEmpty) Iterable(None) else kv._2._20.map(Option(_))
          u <- if (kv._2._21.isEmpty) Iterable(None) else kv._2._21.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
      }
  }

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag, F: ClassTag, G: ClassTag, H: ClassTag, I: ClassTag, J: ClassTag, K: ClassTag, L: ClassTag, M: ClassTag, N: ClassTag, O: ClassTag, P: ClassTag, Q: ClassTag, R: ClassTag, S: ClassTag, T: ClassTag, U: ClassTag, V: ClassTag](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
    coGroup(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      .flatMap { kv =>
        for {
          a <- if (kv._2._1.isEmpty) Iterable(None) else kv._2._1.map(Option(_))
          b <- if (kv._2._2.isEmpty) Iterable(None) else kv._2._2.map(Option(_))
          c <- if (kv._2._3.isEmpty) Iterable(None) else kv._2._3.map(Option(_))
          d <- if (kv._2._4.isEmpty) Iterable(None) else kv._2._4.map(Option(_))
          e <- if (kv._2._5.isEmpty) Iterable(None) else kv._2._5.map(Option(_))
          f <- if (kv._2._6.isEmpty) Iterable(None) else kv._2._6.map(Option(_))
          g <- if (kv._2._7.isEmpty) Iterable(None) else kv._2._7.map(Option(_))
          h <- if (kv._2._8.isEmpty) Iterable(None) else kv._2._8.map(Option(_))
          i <- if (kv._2._9.isEmpty) Iterable(None) else kv._2._9.map(Option(_))
          j <- if (kv._2._10.isEmpty) Iterable(None) else kv._2._10.map(Option(_))
          k <- if (kv._2._11.isEmpty) Iterable(None) else kv._2._11.map(Option(_))
          l <- if (kv._2._12.isEmpty) Iterable(None) else kv._2._12.map(Option(_))
          m <- if (kv._2._13.isEmpty) Iterable(None) else kv._2._13.map(Option(_))
          n <- if (kv._2._14.isEmpty) Iterable(None) else kv._2._14.map(Option(_))
          o <- if (kv._2._15.isEmpty) Iterable(None) else kv._2._15.map(Option(_))
          p <- if (kv._2._16.isEmpty) Iterable(None) else kv._2._16.map(Option(_))
          q <- if (kv._2._17.isEmpty) Iterable(None) else kv._2._17.map(Option(_))
          r <- if (kv._2._18.isEmpty) Iterable(None) else kv._2._18.map(Option(_))
          s <- if (kv._2._19.isEmpty) Iterable(None) else kv._2._19.map(Option(_))
          t <- if (kv._2._20.isEmpty) Iterable(None) else kv._2._20.map(Option(_))
          u <- if (kv._2._21.isEmpty) Iterable(None) else kv._2._21.map(Option(_))
          v <- if (kv._2._22.isEmpty) Iterable(None) else kv._2._22.map(Option(_))
        } yield (kv._1, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
      }
  }

}

// scalastyle:on cyclomatic.complexity
// scalastyle:on file.size.limit
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
// scalastyle:on parameter.number
