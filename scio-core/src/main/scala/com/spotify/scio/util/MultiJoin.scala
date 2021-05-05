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

// generated with multijoin.py

package com.spotify.scio.util

import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.TupleTag

import scala.jdk.CollectionConverters._
import com.spotify.scio.values.SCollection.makePairSCollectionFunctions

trait MultiJoin extends Serializable {

  protected def tfName: String = CallSites.getCurrent

  def toOptions[T](xs: Iterator[T]): Iterator[Option[T]] = if (xs.isEmpty) Iterator(None) else xs.map(Option(_))

  def cogroup[KEY, A, B](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Iterable[A], Iterable[B]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B]) = (a.valueCoder, b.valueCoder)
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

  def cogroup[KEY, A, B, C](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C]) = (a.valueCoder, b.valueCoder, c.valueCoder)
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

  def cogroup[KEY, A, B, C, D](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder)
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

  def cogroup[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U], coderV: Coder[V]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder, v.valueCoder)
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

  def apply[KEY, A, B](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, B))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B]) = (a.valueCoder, b.valueCoder)
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

  def apply[KEY, A, B, C](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, B, C))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C]) = (a.valueCoder, b.valueCoder, c.valueCoder)
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

  def apply[KEY, A, B, C, D](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, B, C, D))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder)
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

  def apply[KEY, A, B, C, D, E](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, B, C, D, E))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, B, C, D, E, F))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, B, C, D, E, F, G))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, B, C, D, E, F, G, H))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder)
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

  def apply[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U], coderV: Coder[V]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder, v.valueCoder)
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

  def left[KEY, A, B](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (A, Option[B]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B]) = (a.valueCoder, b.valueCoder)
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

  def left[KEY, A, B, C](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (A, Option[B], Option[C]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C]) = (a.valueCoder, b.valueCoder, c.valueCoder)
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

  def left[KEY, A, B, C, D](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder)
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

  def left[KEY, A, B, C, D, E](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder)
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

  def left[KEY, A, B, C, D, E, F](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder)
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

  def left[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (A, Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U], coderV: Coder[V]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder, v.valueCoder)
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

  def outer[KEY, A, B](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)]): SCollection[(KEY, (Option[A], Option[B]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B]) = (a.valueCoder, b.valueCoder)
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

  def outer[KEY, A, B, C](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)]): SCollection[(KEY, (Option[A], Option[B], Option[C]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C]) = (a.valueCoder, b.valueCoder, c.valueCoder)
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

  def outer[KEY, A, B, C, D](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder)
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

  def outer[KEY, A, B, C, D, E](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder)
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

  def outer[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](a: SCollection[(KEY, A)], b: SCollection[(KEY, B)], c: SCollection[(KEY, C)], d: SCollection[(KEY, D)], e: SCollection[(KEY, E)], f: SCollection[(KEY, F)], g: SCollection[(KEY, G)], h: SCollection[(KEY, H)], i: SCollection[(KEY, I)], j: SCollection[(KEY, J)], k: SCollection[(KEY, K)], l: SCollection[(KEY, L)], m: SCollection[(KEY, M)], n: SCollection[(KEY, N)], o: SCollection[(KEY, O)], p: SCollection[(KEY, P)], q: SCollection[(KEY, Q)], r: SCollection[(KEY, R)], s: SCollection[(KEY, S)], t: SCollection[(KEY, T)], u: SCollection[(KEY, U)], v: SCollection[(KEY, V)]): SCollection[(KEY, (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H], Option[I], Option[J], Option[K], Option[L], Option[M], Option[N], Option[O], Option[P], Option[Q], Option[R], Option[S], Option[T], Option[U], Option[V]))] = {
    implicit val keyCoder: Coder[KEY] = a.keyCoder
    implicit val (coderA: Coder[A], coderB: Coder[B], coderC: Coder[C], coderD: Coder[D], coderE: Coder[E], coderF: Coder[F], coderG: Coder[G], coderH: Coder[H], coderI: Coder[I], coderJ: Coder[J], coderK: Coder[K], coderL: Coder[L], coderM: Coder[M], coderN: Coder[N], coderO: Coder[O], coderP: Coder[P], coderQ: Coder[Q], coderR: Coder[R], coderS: Coder[S], coderT: Coder[T], coderU: Coder[U], coderV: Coder[V]) = (a.valueCoder, b.valueCoder, c.valueCoder, d.valueCoder, e.valueCoder, f.valueCoder, g.valueCoder, h.valueCoder, i.valueCoder, j.valueCoder, k.valueCoder, l.valueCoder, m.valueCoder, n.valueCoder, o.valueCoder, p.valueCoder, q.valueCoder, r.valueCoder, s.valueCoder, t.valueCoder, u.valueCoder, v.valueCoder)
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
