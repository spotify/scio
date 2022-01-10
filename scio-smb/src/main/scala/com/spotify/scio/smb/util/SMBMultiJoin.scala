/*
 * Copyright 2021 Spotify AB.
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

// generated with smb-multijoin.py

package com.spotify.scio.smb.util

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, TargetParallelism}
import com.spotify.scio.smb.syntax.SortedBucketScioContext

import scala.jdk.CollectionConverters._

final class SMBMultiJoin(private val self: ScioContext) {

  private[this] val sortedBucketScioContext = new SortedBucketScioContext(self)

  def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    targetParallelism: TargetParallelism
  ): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE) =
      (a.getTupleTag, b.getTupleTag, c.getTupleTag, d.getTupleTag, e.getTupleTag)

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E]
  ): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, TargetParallelism.auto())

  def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    targetParallelism: TargetParallelism
  ): SCollection[
    (KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE, tupleTagF) =
      (a.getTupleTag, b.getTupleTag, c.getTupleTag, d.getTupleTag, e.getTupleTag, f.getTupleTag)

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F]
  ): SCollection[
    (KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G])
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE, tupleTagF, tupleTagG) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G]
  ): SCollection[
    (
      KEY,
      (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G])
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE, tupleTagF, tupleTagG, tupleTagH) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H]
      )
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I]
      )
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J]
      )
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K]
      )
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L]
      )
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M]
      )
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N]
      )
    )
  ] =
    sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, TargetParallelism.auto())

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      TargetParallelism.auto()
    )

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .and(p)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala,
            cgbkResult.getAll(tupleTagP).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      TargetParallelism.auto()
    )

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .and(p)
      .and(q)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala,
            cgbkResult.getAll(tupleTagP).asScala,
            cgbkResult.getAll(tupleTagQ).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      TargetParallelism.auto()
    )

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .and(p)
      .and(q)
      .and(r)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala,
            cgbkResult.getAll(tupleTagP).asScala,
            cgbkResult.getAll(tupleTagQ).asScala,
            cgbkResult.getAll(tupleTagR).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      TargetParallelism.auto()
    )

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .and(p)
      .and(q)
      .and(r)
      .and(s)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala,
            cgbkResult.getAll(tupleTagP).asScala,
            cgbkResult.getAll(tupleTagQ).asScala,
            cgbkResult.getAll(tupleTagR).asScala,
            cgbkResult.getAll(tupleTagS).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      TargetParallelism.auto()
    )

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder,
    T: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S],
        Iterable[T]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .and(p)
      .and(q)
      .and(r)
      .and(s)
      .and(t)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS,
      tupleTagT
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag,
      t.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala,
            cgbkResult.getAll(tupleTagP).asScala,
            cgbkResult.getAll(tupleTagQ).asScala,
            cgbkResult.getAll(tupleTagR).asScala,
            cgbkResult.getAll(tupleTagS).asScala,
            cgbkResult.getAll(tupleTagT).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder,
    T: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S],
        Iterable[T]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      t,
      TargetParallelism.auto()
    )

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder,
    T: Coder,
    U: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S],
        Iterable[T],
        Iterable[U]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .and(p)
      .and(q)
      .and(r)
      .and(s)
      .and(t)
      .and(u)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS,
      tupleTagT,
      tupleTagU
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag,
      t.getTupleTag,
      u.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala,
            cgbkResult.getAll(tupleTagP).asScala,
            cgbkResult.getAll(tupleTagQ).asScala,
            cgbkResult.getAll(tupleTagR).asScala,
            cgbkResult.getAll(tupleTagS).asScala,
            cgbkResult.getAll(tupleTagT).asScala,
            cgbkResult.getAll(tupleTagU).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder,
    T: Coder,
    U: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S],
        Iterable[T],
        Iterable[U]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      t,
      u,
      TargetParallelism.auto()
    )

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder,
    T: Coder,
    U: Coder,
    V: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U],
    v: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S],
        Iterable[T],
        Iterable[U],
        Iterable[V]
      )
    )
  ] = {
    val input = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .and(e)
      .and(f)
      .and(g)
      .and(h)
      .and(i)
      .and(j)
      .and(k)
      .and(l)
      .and(m)
      .and(n)
      .and(o)
      .and(p)
      .and(q)
      .and(r)
      .and(s)
      .and(t)
      .and(u)
      .and(v)
      .withTargetParallelism(targetParallelism)
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS,
      tupleTagT,
      tupleTagU,
      tupleTagV
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag,
      t.getTupleTag,
      u.getTupleTag,
      v.getTupleTag
    )

    val tfName = self.tfName

    self
      .applyTransform(s"SMB CoGroupByKey@$tfName", input)
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala,
            cgbkResult.getAll(tupleTagE).asScala,
            cgbkResult.getAll(tupleTagF).asScala,
            cgbkResult.getAll(tupleTagG).asScala,
            cgbkResult.getAll(tupleTagH).asScala,
            cgbkResult.getAll(tupleTagI).asScala,
            cgbkResult.getAll(tupleTagJ).asScala,
            cgbkResult.getAll(tupleTagK).asScala,
            cgbkResult.getAll(tupleTagL).asScala,
            cgbkResult.getAll(tupleTagM).asScala,
            cgbkResult.getAll(tupleTagN).asScala,
            cgbkResult.getAll(tupleTagO).asScala,
            cgbkResult.getAll(tupleTagP).asScala,
            cgbkResult.getAll(tupleTagQ).asScala,
            cgbkResult.getAll(tupleTagR).asScala,
            cgbkResult.getAll(tupleTagS).asScala,
            cgbkResult.getAll(tupleTagT).asScala,
            cgbkResult.getAll(tupleTagU).asScala,
            cgbkResult.getAll(tupleTagV).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[
    KEY: Coder,
    A: Coder,
    B: Coder,
    C: Coder,
    D: Coder,
    E: Coder,
    F: Coder,
    G: Coder,
    H: Coder,
    I: Coder,
    J: Coder,
    K: Coder,
    L: Coder,
    M: Coder,
    N: Coder,
    O: Coder,
    P: Coder,
    Q: Coder,
    R: Coder,
    S: Coder,
    T: Coder,
    U: Coder,
    V: Coder
  ](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U],
    v: SortedBucketIO.Read[V]
  ): SCollection[
    (
      KEY,
      (
        Iterable[A],
        Iterable[B],
        Iterable[C],
        Iterable[D],
        Iterable[E],
        Iterable[F],
        Iterable[G],
        Iterable[H],
        Iterable[I],
        Iterable[J],
        Iterable[K],
        Iterable[L],
        Iterable[M],
        Iterable[N],
        Iterable[O],
        Iterable[P],
        Iterable[Q],
        Iterable[R],
        Iterable[S],
        Iterable[T],
        Iterable[U],
        Iterable[V]
      )
    )
  ] =
    sortMergeCoGroup(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      t,
      u,
      v,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D])
  ] = {
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD) =
      (a.getTupleTag, b.getTupleTag, c.getTupleTag, d.getTupleTag)
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D])
  ] =
    sortMergeTransform(keyClass, a, b, c, d, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E])
  ] = {
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE) =
      (a.getTupleTag, b.getTupleTag, c.getTupleTag, d.getTupleTag, e.getTupleTag)
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E])
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F])
  ] = {
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE, tupleTagF) =
      (a.getTupleTag, b.getTupleTag, c.getTupleTag, d.getTupleTag, e.getTupleTag, f.getTupleTag)
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F])
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G])
  ] = {
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE, tupleTagF, tupleTagG) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G])
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H]
    )
  ] = {
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD, tupleTagE, tupleTagF, tupleTagG, tupleTagH) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H]
    )
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I]
    )
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J]
    )
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K]
    )
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L]
    )
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M]
    )
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N]
    )
  ] =
    sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, TargetParallelism.auto())

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .and(p)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala,
          cgbkResult.getAll(tupleTagP).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .and(p)
        .and(q)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala,
          cgbkResult.getAll(tupleTagP).asScala,
          cgbkResult.getAll(tupleTagQ).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .and(p)
        .and(q)
        .and(r)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala,
          cgbkResult.getAll(tupleTagP).asScala,
          cgbkResult.getAll(tupleTagQ).asScala,
          cgbkResult.getAll(tupleTagR).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .and(p)
        .and(q)
        .and(r)
        .and(s)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala,
          cgbkResult.getAll(tupleTagP).asScala,
          cgbkResult.getAll(tupleTagQ).asScala,
          cgbkResult.getAll(tupleTagR).asScala,
          cgbkResult.getAll(tupleTagS).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S],
      Iterable[T]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS,
      tupleTagT
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag,
      t.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .and(p)
        .and(q)
        .and(r)
        .and(s)
        .and(t)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala,
          cgbkResult.getAll(tupleTagP).asScala,
          cgbkResult.getAll(tupleTagQ).asScala,
          cgbkResult.getAll(tupleTagR).asScala,
          cgbkResult.getAll(tupleTagS).asScala,
          cgbkResult.getAll(tupleTagT).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S],
      Iterable[T]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      t,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S],
      Iterable[T],
      Iterable[U]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS,
      tupleTagT,
      tupleTagU
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag,
      t.getTupleTag,
      u.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .and(p)
        .and(q)
        .and(r)
        .and(s)
        .and(t)
        .and(u)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala,
          cgbkResult.getAll(tupleTagP).asScala,
          cgbkResult.getAll(tupleTagQ).asScala,
          cgbkResult.getAll(tupleTagR).asScala,
          cgbkResult.getAll(tupleTagS).asScala,
          cgbkResult.getAll(tupleTagT).asScala,
          cgbkResult.getAll(tupleTagU).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S],
      Iterable[T],
      Iterable[U]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      t,
      u,
      TargetParallelism.auto()
    )

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U],
    v: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S],
      Iterable[T],
      Iterable[U],
      Iterable[V]
    )
  ] = {
    val (
      tupleTagA,
      tupleTagB,
      tupleTagC,
      tupleTagD,
      tupleTagE,
      tupleTagF,
      tupleTagG,
      tupleTagH,
      tupleTagI,
      tupleTagJ,
      tupleTagK,
      tupleTagL,
      tupleTagM,
      tupleTagN,
      tupleTagO,
      tupleTagP,
      tupleTagQ,
      tupleTagR,
      tupleTagS,
      tupleTagT,
      tupleTagU,
      tupleTagV
    ) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag,
      e.getTupleTag,
      f.getTupleTag,
      g.getTupleTag,
      h.getTupleTag,
      i.getTupleTag,
      j.getTupleTag,
      k.getTupleTag,
      l.getTupleTag,
      m.getTupleTag,
      n.getTupleTag,
      o.getTupleTag,
      p.getTupleTag,
      q.getTupleTag,
      r.getTupleTag,
      s.getTupleTag,
      t.getTupleTag,
      u.getTupleTag,
      v.getTupleTag
    )
    new sortedBucketScioContext.SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(a)
        .and(b)
        .and(c)
        .and(d)
        .and(e)
        .and(f)
        .and(g)
        .and(h)
        .and(i)
        .and(j)
        .and(k)
        .and(l)
        .and(m)
        .and(n)
        .and(o)
        .and(p)
        .and(q)
        .and(r)
        .and(s)
        .and(t)
        .and(u)
        .and(v)
        .withTargetParallelism(targetParallelism),
      cgbkResult =>
        (
          cgbkResult.getAll(tupleTagA).asScala,
          cgbkResult.getAll(tupleTagB).asScala,
          cgbkResult.getAll(tupleTagC).asScala,
          cgbkResult.getAll(tupleTagD).asScala,
          cgbkResult.getAll(tupleTagE).asScala,
          cgbkResult.getAll(tupleTagF).asScala,
          cgbkResult.getAll(tupleTagG).asScala,
          cgbkResult.getAll(tupleTagH).asScala,
          cgbkResult.getAll(tupleTagI).asScala,
          cgbkResult.getAll(tupleTagJ).asScala,
          cgbkResult.getAll(tupleTagK).asScala,
          cgbkResult.getAll(tupleTagL).asScala,
          cgbkResult.getAll(tupleTagM).asScala,
          cgbkResult.getAll(tupleTagN).asScala,
          cgbkResult.getAll(tupleTagO).asScala,
          cgbkResult.getAll(tupleTagP).asScala,
          cgbkResult.getAll(tupleTagQ).asScala,
          cgbkResult.getAll(tupleTagR).asScala,
          cgbkResult.getAll(tupleTagS).asScala,
          cgbkResult.getAll(tupleTagT).asScala,
          cgbkResult.getAll(tupleTagU).asScala,
          cgbkResult.getAll(tupleTagV).asScala
        )
    )
  }

  def sortMergeTransform[KEY, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](
    keyClass: Class[KEY],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    e: SortedBucketIO.Read[E],
    f: SortedBucketIO.Read[F],
    g: SortedBucketIO.Read[G],
    h: SortedBucketIO.Read[H],
    i: SortedBucketIO.Read[I],
    j: SortedBucketIO.Read[J],
    k: SortedBucketIO.Read[K],
    l: SortedBucketIO.Read[L],
    m: SortedBucketIO.Read[M],
    n: SortedBucketIO.Read[N],
    o: SortedBucketIO.Read[O],
    p: SortedBucketIO.Read[P],
    q: SortedBucketIO.Read[Q],
    r: SortedBucketIO.Read[R],
    s: SortedBucketIO.Read[S],
    t: SortedBucketIO.Read[T],
    u: SortedBucketIO.Read[U],
    v: SortedBucketIO.Read[V]
  ): SortedBucketScioContext#SortMergeTransformReadBuilder[
    KEY,
    (
      Iterable[A],
      Iterable[B],
      Iterable[C],
      Iterable[D],
      Iterable[E],
      Iterable[F],
      Iterable[G],
      Iterable[H],
      Iterable[I],
      Iterable[J],
      Iterable[K],
      Iterable[L],
      Iterable[M],
      Iterable[N],
      Iterable[O],
      Iterable[P],
      Iterable[Q],
      Iterable[R],
      Iterable[S],
      Iterable[T],
      Iterable[U],
      Iterable[V]
    )
  ] =
    sortMergeTransform(
      keyClass,
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      q,
      r,
      s,
      t,
      u,
      v,
      TargetParallelism.auto()
    )

}

object SMBMultiJoin {
  final def apply(sc: ScioContext): SMBMultiJoin = new SMBMultiJoin(sc)
}
