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

package com.spotify.scio.smb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.{AbsCoGbkTransform, Transformable}
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.{BucketItem, MergedBucket}
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, SortedBucketTransform, TargetParallelism}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{KV, PCollectionView}

import scala.jdk.CollectionConverters._

trait SortMergeBucketScioContextSyntax {
  implicit def asSMBScioContext(sc: ScioContext): SortedBucketScioContext =
    new SortedBucketScioContext(sc)
}

final class SortedBucketScioContext(@transient private val self: ScioContext) extends Serializable {

  /**
   * Return an SCollection containing all pairs of elements with matching keys in `lhs` and `rhs`.
   * Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `lhs` and
   * (k, v2) is in `rhs`.
   *
   * Unlike a regular [[PairSCollectionFunctions.join]], the key information (namely, how to extract
   * a comparable `K` from `L` and `R`) is remotely encoded in a
   * [[org.apache.beam.sdk.extensions.smb.BucketMetadata]] file in the same directory as the input
   * records. This transform requires a filesystem lookup to ensure that the metadata for each
   * source are compatible. In return for reading pre-sorted data, the shuffle step in a typical
   * [[org.apache.beam.sdk.transforms.GroupByKey]] operation can be eliminated.
   *
   * @group join
   * @param keyClass
   *   join key class. Must have a Coder in Beam's default
   *   [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not supported yet.
   * @param lhs
   * @param rhs
   * @param targetParallelism
   *   the desired parallelism of the job. See
   *   [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
   */
  @experimental
  def sortMergeJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  ): SCollection[(K, (L, R))] = {
    val t = SortedBucketIO.read(keyClass).of(lhs).and(rhs).withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB) = (lhs.getTupleTag, rhs.getTupleTag)
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .applyTransform(ParDo.of(new DoFn[KV[K, CoGbkResult], (K, (L, R))] {
        @ProcessElement
        private[smb] def processElement(
          c: DoFn[KV[K, CoGbkResult], (K, (L, R))]#ProcessContext
        ): Unit = {
          val cgbkResult = c.element().getValue
          val (resA, resB) = (cgbkResult.getAll(tupleTagA), cgbkResult.getAll(tupleTagB))
          val itB = resB.iterator()
          val key = c.element().getKey

          while (itB.hasNext) {
            val b = itB.next()
            val ai = resA.iterator()
            while (ai.hasNext) {
              val a = ai.next()
              c.output((key, (a, b)))
            }
          }
        }
      }))
  }

  /** Secondary keyed variant. */
  @experimental
  def sortMergeJoin[K1: Coder, K2: Coder, L: Coder, R: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), (L, R))] = {
    val t = SortedBucketIO
      .read(keyClass, keyClassSecondary)
      .of(lhs)
      .and(rhs)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB) = (lhs.getTupleTag, rhs.getTupleTag)
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .applyTransform(ParDo.of(new DoFn[KV[KV[K1, K2], CoGbkResult], ((K1, K2), (L, R))] {
        @ProcessElement
        private[smb] def processElement(
          c: DoFn[KV[KV[K1, K2], CoGbkResult], ((K1, K2), (L, R))]#ProcessContext
        ): Unit = {
          val cgbkResult = c.element().getValue
          val (resA, resB) = (cgbkResult.getAll(tupleTagA), cgbkResult.getAll(tupleTagB))
          val itB = resB.iterator()
          val k = c.element().getKey
          val outKey = (k.getKey, k.getValue)

          while (itB.hasNext) {
            val b = itB.next()
            val ai = resA.iterator()
            while (ai.hasNext) {
              val a = ai.next()
              c.output((outKey, (a, b)))
            }
          }
        }
      }))
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeJoin[K1: Coder, K2: Coder, L: Coder, R: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R]
  ): SCollection[((K1, K2), (L, R))] =
    sortMergeJoin(keyClass, keyClassSecondary, lhs, rhs, TargetParallelism.auto())

  /**
   * For each key K in `read` return a resulting SCollection that contains a tuple with the list of
   * values for that key in `read`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin]] for information on how an SMB group
   * differs from a regular [[org.apache.beam.sdk.transforms.GroupByKey]] operation.
   *
   * @group per_key
   *
   * @param keyClass
   *   cogroup key class. Must have a Coder in Beam's default
   *   [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not supported yet.
   * @param targetParallelism
   *   the desired parallelism of the job. See
   *   [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
   */
  @experimental
  def sortMergeGroupByKey[K: Coder, V: Coder](
    keyClass: Class[K],
    read: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  ): SCollection[(K, Iterable[V])] = {
    val t = SortedBucketIO.read(keyClass).of(read).withTargetParallelism(targetParallelism)
    val tupleTag = read.getTupleTag
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB GroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        (
          kv.getKey,
          kv.getValue.getAll(tupleTag).asScala
        )
      }
  }

  /** Secondary keyed variant. */
  @experimental
  def sortMergeGroupByKey[K1: Coder, K2: Coder, V: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    read: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), Iterable[V])] = {
    val t = SortedBucketIO
      .read(keyClass, keyClassSecondary)
      .of(read)
      .withTargetParallelism(targetParallelism)
    val tupleTag = read.getTupleTag
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB GroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        val k = kv.getKey
        (
          (k.getKey, k.getValue),
          kv.getValue.getAll(tupleTag).asScala
        )
      }
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeGroupByKey[K1: Coder, K2: Coder, V: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    read: SortedBucketIO.Read[V]
  ): SCollection[((K1, K2), Iterable[V])] =
    sortMergeGroupByKey(keyClass, keyClassSecondary, read, TargetParallelism.auto())

  /**
   * For each key K in `a` or `b` return a resulting SCollection that contains a tuple with the list
   * of values for that key in `a`, and `b`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.transforms.join.CoGroupByKey]] operation.
   *
   * @group cogroup
   *
   * @param keyClass
   *   cogroup key class. Must have a Coder in Beam's default
   *   [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not supported yet.
   * @param targetParallelism
   *   the desired parallelism of the job. See
   *   [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
   */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  ): SCollection[(K, (Iterable[A], Iterable[B]))] = {
    val t = SortedBucketIO.read(keyClass).of(a).and(b).withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB) = (
      a.getTupleTag,
      b.getTupleTag
    )
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue

        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala
          )
        )
      }
  }

  /** Secondary keyed variant. */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B]))] = {
    val t = SortedBucketIO
      .read(keyClass, keyClassSecondary)
      .of(a)
      .and(b)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB) = (
      a.getTupleTag,
      b.getTupleTag
    )
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        val k = kv.getKey

        (
          (k.getKey, k.getValue),
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala
          )
        )
      }
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B]))] =
    sortMergeCoGroup(keyClass, keyClassSecondary, a, b, TargetParallelism.auto())

  /**
   * For each key K in `a` or `b` or `c`, return a resulting SCollection that contains a tuple with
   * the list of values for that key in `a`, `b` and `c`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.transforms.join.CoGroupByKey]] operation.
   *
   * @group cogroup
   *
   * @param keyClass
   *   cogroup key class. Must have a Coder in Beam's default
   *   [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not supported yet.
   * @param targetParallelism
   *   the desired parallelism of the job. See
   *   [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
   */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] = {
    val t = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag
    )
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue

        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala
          )
        )
      }
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] =
    sortMergeCoGroup(keyClass, a, b, c, TargetParallelism.auto())

  /** Secondary keyed variant */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B], Iterable[C]))] = {
    val t = SortedBucketIO
      .read(keyClass, keyClassSecondary)
      .of(a)
      .and(b)
      .and(c)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag
    )
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        val k = kv.getKey

        (
          (k.getKey, k.getValue),
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala
          )
        )
      }
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B], Iterable[C]))] =
    sortMergeCoGroup(keyClass, keyClassSecondary, a, b, c)

  /**
   * For each key K in `a` or `b` or `c` or `d`, return a resulting SCollection that contains a
   * tuple with the list of values for that key in `a`, `b`, `c` and `d`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.transforms.join.CoGroupByKey]] operation.
   *
   * @group cogroup
   *
   * @param keyClass
   *   cogroup key class. Must have a Coder in Beam's default
   *   [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not supported yet.
   * @param targetParallelism
   *   the desired parallelism of the job. See
   *   [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
   */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    targetParallelism: TargetParallelism
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    val t = SortedBucketIO
      .read(keyClass)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag
    )
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue

        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala
          )
        )
      }
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] =
    sortMergeCoGroup(keyClass, a, b, c, d, TargetParallelism.auto())

  /** Secondary keyed variant */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    val t = SortedBucketIO
      .read(keyClass, keyClassSecondary)
      .of(a)
      .and(b)
      .and(c)
      .and(d)
      .withTargetParallelism(targetParallelism)
    val (tupleTagA, tupleTagB, tupleTagC, tupleTagD) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag,
      d.getTupleTag
    )
    val tfName = self.tfName

    self
      .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      .withName(tfName)
      .map { kv =>
        val cgbkResult = kv.getValue
        val k = kv.getKey

        (
          (k.getKey, k.getValue),
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala,
            cgbkResult.getAll(tupleTagD).asScala
          )
        )
      }
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D]
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] =
    sortMergeCoGroup(keyClass, keyClassSecondary, a, b, c, d, TargetParallelism.auto())

  /**
   * Perform a [[SortedBucketScioContext.sortMergeGroupByKey]] operation, then immediately apply a
   * transformation function to the merged groups and re-write using the same bucketing key and
   * hashing scheme. By applying the write, transform, and write in the same transform, an extra
   * shuffle step can be avoided.
   *
   * @group per_key
   */
  @experimental
  def sortMergeTransform[K, R](
    keyClass: Class[K],
    read: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism
  ): SortMergeTransformReadBuilder[K, K, Void, Iterable[R]] = {
    val tupleTag = read.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO.read(keyClass).of(read).withTargetParallelism(targetParallelism),
      _.getAll(tupleTag).asScala
    )
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K, R](
    keyClass: Class[K],
    read: SortedBucketIO.Read[R]
  ): SortMergeTransformReadBuilder[K, K, Void, Iterable[R]] =
    sortMergeTransform(keyClass, read, TargetParallelism.auto())

  /** Secondary keyed variant */
  @experimental
  def sortMergeTransform[K1, K2, R](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    read: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism
  ): SortMergeTransformReadBuilder[KV[K1, K2], K1, K2, Iterable[R]] = {
    val tupleTag = read.getTupleTag
    new SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass, keyClassSecondary)
        .of(read)
        .withTargetParallelism(targetParallelism),
      _.getAll(tupleTag).asScala
    )
  }

  /**
   * Perform a 2-way [[SortedBucketScioContext.sortMergeCoGroup]] operation, then immediately apply
   * a transformation function to the merged cogroups and re-write using the same bucketing key and
   * hashing scheme. By applying the write, transform, and write in the same transform, an extra
   * shuffle step can be avoided.
   *
   * @group cogroup
   */
  @experimental
  def sortMergeTransform[K, A, B](
    keyClass: Class[K],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism
  ): SortMergeTransformReadBuilder[K, K, Void, (Iterable[A], Iterable[B])] = {
    val tupleTagA = readA.getTupleTag
    val tupleTagB = readB.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO.read(keyClass).of(readA).and(readB).withTargetParallelism(targetParallelism),
      cgbk => (cgbk.getAll(tupleTagA).asScala, cgbk.getAll(tupleTagB).asScala)
    )
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K, A, B](
    keyClass: Class[K],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B]
  ): SortMergeTransformReadBuilder[K, K, Void, (Iterable[A], Iterable[B])] =
    sortMergeTransform(keyClass, readA, readB, TargetParallelism.auto())

  /** Secondary keyed variant */
  @experimental
  def sortMergeTransform[K1, K2, A, B](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism
  ): SortMergeTransformReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B])] = {
    val tupleTagA = readA.getTupleTag
    val tupleTagB = readB.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass, keyClassSecondary)
        .of(readA)
        .and(readB)
        .withTargetParallelism(targetParallelism),
      cgbk => (cgbk.getAll(tupleTagA).asScala, cgbk.getAll(tupleTagB).asScala)
    )
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K1, K2, A, B](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B]
  ): SortMergeTransformReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B])] =
    sortMergeTransform(keyClass, keyClassSecondary, readA, readB)

  /**
   * Perform a 3-way [[SortedBucketScioContext.sortMergeCoGroup]] operation, then immediately apply
   * a transformation function to the merged cogroups and re-write using the same bucketing key and
   * hashing scheme. By applying the write, transform, and write in the same transform, an extra
   * shuffle step can be avoided.
   *
   * @group cogroup
   */
  @experimental
  def sortMergeTransform[K, A, B, C](
    keyClass: Class[K],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B],
    readC: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism
  ): SortMergeTransformReadBuilder[K, K, Void, (Iterable[A], Iterable[B], Iterable[C])] = {
    val tupleTagA = readA.getTupleTag
    val tupleTagB = readB.getTupleTag
    val tupleTagC = readC.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass)
        .of(readA)
        .and(readB)
        .and(readC)
        .withTargetParallelism(targetParallelism),
      cgbk =>
        (
          cgbk.getAll(tupleTagA).asScala,
          cgbk.getAll(tupleTagB).asScala,
          cgbk.getAll(tupleTagC).asScala
        )
    )
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K, A, B, C](
    keyClass: Class[K],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B],
    readC: SortedBucketIO.Read[C]
  ): SortMergeTransformReadBuilder[K, K, Void, (Iterable[A], Iterable[B], Iterable[C])] =
    sortMergeTransform(keyClass, readA, readB, readC, TargetParallelism.auto())

  /** Secondary keyed variant */
  @experimental
  def sortMergeTransform[K1, K2, A, B, C](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B],
    readC: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism
  ): SortMergeTransformReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B], Iterable[C])] = {
    val tupleTagA = readA.getTupleTag
    val tupleTagB = readB.getTupleTag
    val tupleTagC = readC.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO
        .read(keyClass, keyClassSecondary)
        .of(readA)
        .and(readB)
        .and(readC)
        .withTargetParallelism(targetParallelism),
      cgbk =>
        (
          cgbk.getAll(tupleTagA).asScala,
          cgbk.getAll(tupleTagB).asScala,
          cgbk.getAll(tupleTagC).asScala
        )
    )
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K1, K2, A, B, C](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    readA: SortedBucketIO.Read[A],
    readB: SortedBucketIO.Read[B],
    readC: SortedBucketIO.Read[C]
  ): SortMergeTransformReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B], Iterable[C])] =
    sortMergeTransform(keyClass, keyClassSecondary, readA, readB, readC, TargetParallelism.auto())

  class SortMergeTransformReadBuilder[KeyType, K1, K2, R](
    coGbk: Transformable[KeyType, K1, K2],
    toR: CoGbkResult => R
  ) extends Serializable {
    def to[W: Coder](
      output: SortedBucketIO.TransformOutput[K1, K2, W]
    ): SortMergeTransformWriteBuilder[KeyType, R, W] =
      new SortMergeTransformWriteBuilder(coGbk.transform(output), toR)
  }

  class SortMergeTransformWriteBuilder[KeyType, R, W](
    transform: AbsCoGbkTransform[KeyType, W],
    toR: CoGbkResult => R
  ) {
    def withSideInputs(
      sides: SideInput[_]*
    ): SortMergeTransformWithSideInputsWriteBuilder[KeyType, R, W] =
      new SortMergeTransformWithSideInputsWriteBuilder(transform, toR, sides)

    /**
     * Defines the transforming function applied to each key group, where the output(s) are sent to
     * the provided consumer via its `accept` function.
     *
     * The key group is defined as a key K and records R, where R represents the unpacked
     * [[CoGbkResult]] converted to Scala Iterables. Note that, unless a
     * [[org.apache.beam.sdk.extensions.smb.SortedBucketSource.Predicate]] is provided to the
     * PTransform, the Scala Iterable is backed by a lazy iterator, and will only materialize if
     * .toList or similar function is used in the transformFn. If you have extremely large key
     * groups, take care to only materialize as much of the Iterable as is needed.
     */
    def via(
      transformFn: (KeyType, R, SortedBucketTransform.SerializableConsumer[W]) => Unit
    ): ClosedTap[Nothing] = {
      val fn = new SortedBucketTransform.TransformFn[KeyType, W]() {
        override def writeTransform(
          keyGroup: KV[KeyType, CoGbkResult],
          outputConsumer: SortedBucketTransform.SerializableConsumer[W]
        ): Unit =
          transformFn.apply(
            keyGroup.getKey,
            toR(keyGroup.getValue),
            outputConsumer
          )
      }

      val t = transform.via(fn)
      val tfName = self.tfName(Some("sortMergeTransform"))
      self.applyInternal(tfName, t)
      ClosedTap[Nothing](EmptyTap)
    }
  }

  class SortMergeTransformWithSideInputsWriteBuilder[KeyType, R, W](
    transform: AbsCoGbkTransform[KeyType, W],
    toR: CoGbkResult => R,
    sides: Iterable[SideInput[_]]
  ) extends Serializable {
    def via(
      transformFn: (
        KeyType,
        R,
        SideInputContext[_],
        SortedBucketTransform.SerializableConsumer[W]
      ) => Unit
    ): ClosedTap[Nothing] = {
      val sideViews: java.lang.Iterable[PCollectionView[_]] = sides.map(_.view).asJava

      val fn = new SortedBucketTransform.TransformFnWithSideInputContext[KeyType, W]() {
        override def writeTransform(
          keyGroup: KV[KeyType, CoGbkResult],
          c: DoFn[BucketItem, MergedBucket]#ProcessContext,
          outputConsumer: SortedBucketTransform.SerializableConsumer[W],
          window: BoundedWindow
        ): Unit = {
          val ctx =
            new SideInputContext(c.asInstanceOf[DoFn[BucketItem, AnyRef]#ProcessContext], window)
          transformFn.apply(
            keyGroup.getKey,
            toR(keyGroup.getValue),
            ctx,
            outputConsumer
          )
        }
      }
      val t = transform.via(fn, sideViews)
      self.applyInternal(t)
      ClosedTap[Nothing](EmptyTap)
    }
  }
}
