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
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.SortedBucketIO
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.KV

import scala.collection.JavaConverters._

trait SortMergeBucketScioContextSyntax {
  implicit def asSMBScioContext(sc: ScioContext): SortedBucketScioContext =
    new SortedBucketScioContext(sc)
}

final class SortedBucketScioContext(@transient private val self: ScioContext) {

  /**
   * Return an SCollection containing all pairs of elements with matching keys in `lhs` and
   * `rhs`. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in
   * `lhs` and (k, v2) is in `rhs`.
   *
   * Unlike a regular [[PairSCollectionFunctions.join()]], the key information (namely, how to
   * extract a comparable `K` from `L` and `R`) is remotely encoded in a
   * [[org.apache.beam.sdk.extensions.smb.BucketMetadata]] file in the same directory as the
   * input records. This transform requires a filesystem lookup to ensure that the metadata for
   * each source are compatible.
   *
   * @group join
   * @param keyClass join key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   * @param lhs
   * @param rhs
   */
  def sortMergeJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R]
  ): SCollection[(K, (L, R))] = {
    val t = SortedBucketIO.read(keyClass).of(lhs).and(rhs)
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
   * For each key K in `a` or `b` return a resulting SCollection that contains a tuple with the
   * list of values for that key in `a`, and `b`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin()]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.schemas.transforms.CoGroup]] operation.
   *
   * @group cogroup

   * @param keyClass cogroup key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   */
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SCollection[(K, (Iterable[A], Iterable[B]))] = {
    val t = SortedBucketIO.read(keyClass).of(a).and(b)
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

  /**
   * For each key K in `a` or `b` or `c`, return a resulting SCollection that contains a tuple
   * with the list of values for that key in `a`, `b` and `c`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin()]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.schemas.transforms.CoGroup]] operation.
   *
   * @group cogroup

   * @param keyClass cogroup key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   */
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] = {
    val t = SortedBucketIO.read(keyClass).of(a).and(b).and(c)
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

  /**
   * For each key K in `a` or `b` or `c` or `d`, return a resulting SCollection that contains a
   * tuple with the list of values for that key in `a`, `b`, `c` and `d`.
   *
   * See note on [[SortedBucketScioContext.sortMergeJoin()]] for information on how an SMB cogroup
   * differs from a regular [[org.apache.beam.sdk.schemas.transforms.CoGroup]] operation.
   *
   * @group cogroup

   * @param keyClass cogroup key class. Must have a Coder in Beam's default
   *                 [[org.apache.beam.sdk.coders.CoderRegistry]] as custom key coders are not
   *                 supported yet.
   */
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    val t = SortedBucketIO.read(keyClass).of(a).and(b).and(c).and(d)
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
}
