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

import com.spotify.scio.coders.Coder


import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.TupleTag

import scala.collection.JavaConverters._

trait MultiJoin extends Serializable {

  protected def tfName: String = CallSites.getCurrent

  def toOptions[T](xs: Iterator[T]): Iterator[Option[T]] = if (xs.isEmpty) Iterator(None) else xs.map(Option(_))

  def cogroup[KEY: Coder, A: Coder, B: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Iterable[A], Iterable[B]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K]))] = {
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

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala, result.getAll(tagT).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala, result.getAll(tagT).asScala, result.getAll(tagU).asScala))
    }
  }

  def cogroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).map { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      (key, (result.getAll(tagA).asScala, result.getAll(tagB).asScala, result.getAll(tagC).asScala, result.getAll(tagD).asScala, result.getAll(tagE).asScala, result.getAll(tagF).asScala, result.getAll(tagG).asScala, result.getAll(tagH).asScala, result.getAll(tagI).asScala, result.getAll(tagJ).asScala, result.getAll(tagK).asScala, result.getAll(tagL).asScala, result.getAll(tagM).asScala, result.getAll(tagN).asScala, result.getAll(tagO).asScala, result.getAll(tagP).asScala, result.getAll(tagQ).asScala, result.getAll(tagR).asScala, result.getAll(tagS).asScala, result.getAll(tagT).asScala, result.getAll(tagU).asScala, result.getAll(tagV).asScala))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, B))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, B, C))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, B, C, D))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, B, C, D, E))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, B, C, D, E, F))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, B, C, D, E, F, G))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, B, C, D, E, F, G, H))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K))] = {
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

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        p <- result.getAll(tagP).asScala.iterator
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        q <- result.getAll(tagQ).asScala.iterator
        p <- result.getAll(tagP).asScala.iterator
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        r <- result.getAll(tagR).asScala.iterator
        q <- result.getAll(tagQ).asScala.iterator
        p <- result.getAll(tagP).asScala.iterator
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        s <- result.getAll(tagS).asScala.iterator
        r <- result.getAll(tagR).asScala.iterator
        q <- result.getAll(tagQ).asScala.iterator
        p <- result.getAll(tagP).asScala.iterator
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        t <- result.getAll(tagT).asScala.iterator
        s <- result.getAll(tagS).asScala.iterator
        r <- result.getAll(tagR).asScala.iterator
        q <- result.getAll(tagQ).asScala.iterator
        p <- result.getAll(tagP).asScala.iterator
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        u <- result.getAll(tagU).asScala.iterator
        t <- result.getAll(tagT).asScala.iterator
        s <- result.getAll(tagS).asScala.iterator
        r <- result.getAll(tagR).asScala.iterator
        q <- result.getAll(tagQ).asScala.iterator
        p <- result.getAll(tagP).asScala.iterator
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
    }
  }

  def apply[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        v <- result.getAll(tagV).asScala.iterator
        u <- result.getAll(tagU).asScala.iterator
        t <- result.getAll(tagT).asScala.iterator
        s <- result.getAll(tagS).asScala.iterator
        r <- result.getAll(tagR).asScala.iterator
        q <- result.getAll(tagQ).asScala.iterator
        p <- result.getAll(tagP).asScala.iterator
        o <- result.getAll(tagO).asScala.iterator
        n <- result.getAll(tagN).asScala.iterator
        m <- result.getAll(tagM).asScala.iterator
        l <- result.getAll(tagL).asScala.iterator
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, Option[B]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, Option[B], Option[C]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
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

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        t <- toOptions(result.getAll(tagT).asScala.iterator)
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        u <- toOptions(result.getAll(tagU).asScala.iterator)
        t <- toOptions(result.getAll(tagT).asScala.iterator)
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
    }
  }

  def left[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        v <- toOptions(result.getAll(tagV).asScala.iterator)
        u <- toOptions(result.getAll(tagU).asScala.iterator)
        t <- toOptions(result.getAll(tagT).asScala.iterator)
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Option[A], Option[B]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Option[A], Option[B], Option[C]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
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

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        t <- toOptions(result.getAll(tagT).asScala.iterator)
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        u <- toOptions(result.getAll(tagU).asScala.iterator)
        t <- toOptions(result.getAll(tagT).asScala.iterator)
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u))
    }
  }

  def outer[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
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
      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())
    a.context.wrap(keyed).withName(tfName).flatMap { kv =>
      val (key, result) = (kv.getKey, kv.getValue)
      for {
        v <- toOptions(result.getAll(tagV).asScala.iterator)
        u <- toOptions(result.getAll(tagU).asScala.iterator)
        t <- toOptions(result.getAll(tagT).asScala.iterator)
        s <- toOptions(result.getAll(tagS).asScala.iterator)
        r <- toOptions(result.getAll(tagR).asScala.iterator)
        q <- toOptions(result.getAll(tagQ).asScala.iterator)
        p <- toOptions(result.getAll(tagP).asScala.iterator)
        o <- toOptions(result.getAll(tagO).asScala.iterator)
        n <- toOptions(result.getAll(tagN).asScala.iterator)
        m <- toOptions(result.getAll(tagM).asScala.iterator)
        l <- toOptions(result.getAll(tagL).asScala.iterator)
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
      } yield (key, (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v))
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
