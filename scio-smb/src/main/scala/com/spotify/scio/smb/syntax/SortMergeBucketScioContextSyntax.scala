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
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, SortedBucketTransform, TargetParallelism}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.KV

import scala.jdk.CollectionConverters._

trait SortMergeBucketScioContextSyntax {
  implicit def asSMBScioContext(sc: ScioContext): SortedBucketScioContext =
    new SortedBucketScioContext(sc)
}

final class SortedBucketScioContext(@transient private val self: ScioContext) extends Serializable {

  /**
   * Return an SCollection containing all pairs of elements with matching keys in `lhs` and
   * `rhs`. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in
   * `lhs` and (k, v2) is in `rhs`.
   *
   * Unlike a regular [[PairSCollectionFunctions.join()]], the key information (namely, how to
   * extract a comparable `K` from `L` and `R`) is remotely encoded in a
   * [[org.apache.beam.sdk.extensions.smb.BucketMetadata]] file in the same directory as the
   * input records. This transform requires a filesystem lookup to ensure that the metadata for
   * each source are compatible. In return for reading pre-sorted data, the shuffle step in a
   * typical [[org.apache.beam.sdk.transforms.GroupByKey]] operation can be eliminated.
   *
   * @group join
   * @param keyClass join key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   * @param lhs
   * @param rhs
   * @param targetParallelism the desired parallelism of the job. See
   *                 [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
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

  /**
   * For each key K in `read` return a resulting SCollection that contains a tuple with the
   * list of values for that key in `read`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin()]] for information on how an SMB group
   * differs from a regular [[org.apache.beam.sdk.transforms.GroupByKey]] operation.
   *
   * @group per_key

   * @param keyClass grouping key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   */
  @experimental
  def sortMergeGroupByKey[K: Coder, V: Coder](
    keyClass: Class[K],
    read: SortedBucketIO.Read[V]
  ): SCollection[(K, Iterable[V])] = {
    val t = SortedBucketIO.read(keyClass).of(read)
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

  /**
   * For each key K in `a` or `b` return a resulting SCollection that contains a tuple with the
   * list of values for that key in `a`, and `b`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin()]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.transforms.join.CoGroupByKey]] operation.
   *
   * @group cogroup

   * @param keyClass cogroup key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   * @param targetParallelism the desired parallelism of the job. See
   *                 [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
   */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism
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

  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SCollection[(K, (Iterable[A], Iterable[B]))] =
    sortMergeCoGroup(keyClass, a, b, TargetParallelism.auto())

  /**
   * For each key K in `a` or `b` or `c`, return a resulting SCollection that contains a tuple
   * with the list of values for that key in `a`, `b` and `c`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin()]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.transforms.join.CoGroupByKey]] operation.
   *
   * @group cogroup

   * @param keyClass cogroup key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   * @param targetParallelism the desired parallelism of the job. See
   *                 [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
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

  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] =
    sortMergeCoGroup(keyClass, a, b, c, TargetParallelism.auto())

  /**
   * For each key K in `a` or `b` or `c` or `d`, return a resulting SCollection that contains a
   * tuple with the list of values for that key in `a`, `b`, `c` and `d`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin()]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.transforms.join.CoGroupByKey]] operation.
   *
   * @group cogroup

   * @param keyClass cogroup key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   * @param targetParallelism the desired parallelism of the job. See
   *                 [[org.apache.beam.sdk.extensions.smb.TargetParallelism]] for more information.
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
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] =
    sortMergeCoGroup(keyClass, a, b, c, d, TargetParallelism.auto())

  /**
   * Perform a [[SortedBucketScioContext.sortMergeGroupByKey()]] operation, then immediately apply
   * a transformation function to the merged groups and re-write using the same bucketing key and
   * hashing scheme. By applying the write, transform, and write in the same transform, an extra
   * shuffle step can be avoided.
   *
   * @group per_key
   */
  @experimental
  def sortMergeTransform[K, R](
    keyClass: Class[K],
    read: => SortedBucketIO.Read[R]
  ): SortMergeTransformReadBuilder[K, Iterable[R]] = {
    val tupleTag = read.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO.read(keyClass).of(read),
      _.getAll(tupleTag).asScala
    )
  }

  /**
   * Perform a 2-way [[SortedBucketScioContext.sortMergeCoGroup()]] operation, then immediately
   * apply a transformation function to the merged cogroups and re-write using the same bucketing
   * key and hashing scheme. By applying the write, transform, and write in the same transform,
   * an extra shuffle step can be avoided.
   *
   * @group cogroup
   */
  @experimental
  def sortMergeTransform[K, A, B](
    keyClass: Class[K],
    readA: => SortedBucketIO.Read[A],
    readB: => SortedBucketIO.Read[B]
  ): SortMergeTransformReadBuilder[K, (Iterable[A], Iterable[B])] = {
    val tupleTagA = readA.getTupleTag
    val tupleTagB = readB.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO.read(keyClass).of(readA).and(readB),
      cgbk => (cgbk.getAll(tupleTagA).asScala, cgbk.getAll(tupleTagB).asScala)
    )
  }

  /**
   * Perform a 3-way [[SortedBucketScioContext.sortMergeCoGroup()]] operation, then immediately
   * apply a transformation function to the merged cogroups and re-write using the same bucketing
   * key and hashing scheme. By applying the write, transform, and write in the same transform,
   * an extra shuffle step can be avoided.
   *
   * @group cogroup
   */
  @experimental
  def sortMergeTransform[K, A, B, C](
    keyClass: Class[K],
    readA: => SortedBucketIO.Read[A],
    readB: => SortedBucketIO.Read[B],
    readC: => SortedBucketIO.Read[C]
  ): SortMergeTransformReadBuilder[K, (Iterable[A], Iterable[B], Iterable[C])] = {
    val tupleTagA = readA.getTupleTag
    val tupleTagB = readB.getTupleTag
    val tupleTagC = readC.getTupleTag

    new SortMergeTransformReadBuilder(
      SortedBucketIO.read(keyClass).of(readA).and(readB).and(readC),
      cgbk =>
        (
          cgbk.getAll(tupleTagA).asScala,
          cgbk.getAll(tupleTagB).asScala,
          cgbk.getAll(tupleTagC).asScala
        )
    )
  }

  class SortMergeTransformReadBuilder[K, R](
    coGbk: => SortedBucketIO.CoGbk[K],
    toR: CoGbkResult => R
  ) extends Serializable {

    def to[W: Coder](
      write: => SortedBucketIO.Write[K, W]
    ): SortMergeTransformWriteBuilder[K, R, W] =
      new SortMergeTransformWriteBuilder(coGbk.to(write), toR)
  }

  class SortMergeTransformWriteBuilder[K, R, W](
    transform: => SortedBucketIO.CoGbkTransform[K, W],
    toR: CoGbkResult => R
  ) extends Serializable {

    def via(
      transformFn: (K, R, SortedBucketTransform.SerializableConsumer[W]) => Unit
    ): ClosedTap[Nothing] = {
      val fn = new SortedBucketTransform.TransformFn[K, W]() {
        override def writeTransform(
          keyGroup: KV[K, CoGbkResult],
          outputConsumer: SortedBucketTransform.SerializableConsumer[W]
        ): Unit =
          transformFn.apply(
            keyGroup.getKey,
            toR(keyGroup.getValue),
            outputConsumer
          )
      }

      val t = transform.via(fn)

      self.applyInternal(t)
      ClosedTap[Nothing](EmptyTap)
    }
  }
}
