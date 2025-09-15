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
import com.spotify.scio.smb.SortMergeTransform
import com.spotify.scio.smb.util.SMBMultiJoin
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, SortedBucketIOUtil, TargetParallelism}
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement}
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
  ): SCollection[(K, (L, R))] = self.requireNotClosed {
    if (self.isTest) {
      testJoin(lhs, rhs)
    } else {
      val t = SortedBucketIO
        .read(keyClass)
        .of(lhs, rhs)
        .withTargetParallelism(targetParallelism)
      val (tupleTagA, tupleTagB) = (lhs.getTupleTag, rhs.getTupleTag)
      val tfName = self.tfName

      self
        .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
        .withName(tfName)
        .applyTransform(ParDo.of(new DoFn[KV[K, CoGbkResult], (K, (L, R))] {
          @ProcessElement
          private[smb] def processElement(
            @Element element: KV[K, CoGbkResult],
            out: OutputReceiver[(K, (L, R))]
          ): Unit = {
            val cgbkResult = element.getValue
            val (resA, resB) = (cgbkResult.getAll(tupleTagA), cgbkResult.getAll(tupleTagB))
            val itB = resB.iterator()
            val key = element.getKey

            while (itB.hasNext) {
              val b = itB.next()
              val ai = resA.iterator()
              while (ai.hasNext) {
                val a = ai.next()
                out.output((key, (a, b)))
              }
            }
          }
        }))
    }
  }

  /** Secondary keyed variant. */
  @experimental
  def sortMergeJoin[K1: Coder, K2: Coder, L: Coder, R: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), (L, R))] = self.requireNotClosed {
    if (self.isTest) {
      testJoin(lhs, rhs)
    } else {
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
            @Element element: KV[KV[K1, K2], CoGbkResult],
            out: OutputReceiver[((K1, K2), (L, R))]
          ): Unit = {
            val cgbkResult = element.getValue
            val (resA, resB) = (cgbkResult.getAll(tupleTagA), cgbkResult.getAll(tupleTagB))
            val itB = resB.iterator()
            val k = element.getKey
            val outKey = (k.getKey, k.getValue)

            while (itB.hasNext) {
              val b = itB.next()
              val ai = resA.iterator()
              while (ai.hasNext) {
                val a = ai.next()
                out.output((outKey, (a, b)))
              }
            }
          }
        }))
    }
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

  private def testJoin[K, L, R](
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R]
  ): SCollection[(K, (L, R))] = {
    val testInput = TestDataManager.getInput(self.testId.get)
    val testLhs = testInput[(K, L)](SortedBucketIOUtil.testId(lhs)).toSCollection(self)
    val testRhs = testInput[(K, R)](SortedBucketIOUtil.testId(rhs)).toSCollection(self)
    testLhs.join(testRhs)
  }

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
  ): SCollection[(K, Iterable[V])] = self.requireNotClosed {
    if (self.isTest) {
      testCoGroupByKey(read)
    } else {
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
  }

  /** Secondary keyed variant. */
  @experimental
  def sortMergeGroupByKey[K1: Coder, K2: Coder, V: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    read: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), Iterable[V])] = self.requireNotClosed {
    if (self.isTest) {
      testCoGroupByKey(read)
    } else {
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
  }

  private def testCoGroupByKey[K, V](
    read: SortedBucketIO.Read[V]
  ): SCollection[(K, Iterable[V])] = {
    val testInput = TestDataManager.getInput(self.testId.get)
    testInput[(K, V)](SortedBucketIOUtil.testId(read)).toSCollection(self).groupByKey
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
  ): SCollection[(K, (Iterable[A], Iterable[B]))] =
    SMBMultiJoin(self).sortMergeCoGroup(keyClass, a, b, targetParallelism)

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SCollection[(K, (Iterable[A], Iterable[B]))] =
    SMBMultiJoin(self).sortMergeCoGroup(keyClass, a, b)

  /** Secondary keyed variant. */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B]))] = self.requireNotClosed {
    val tfName = self.tfName
    val keyed = if (self.isTest) {
      SMBMultiJoin(self).testCoGroup[KV[K1, K2]](a, b)
    } else {
      val t = SortedBucketIO
        .read(keyClass, keyClassSecondary)
        .of(a)
        .and(b)
        .withTargetParallelism(targetParallelism)

      self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
    }

    keyed
      .withName(tfName)
      .map { kv =>
        val k = kv.getKey
        val k1 = k.getKey
        val k2 = k.getValue
        val cgbkResult = kv.getValue
        val asForK = cgbkResult.getAll(a.getTupleTag).asScala
        val bsForK = cgbkResult.getAll(b.getTupleTag).asScala
        (k1, k2) -> ((asForK, bsForK))
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
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] =
    SMBMultiJoin(self).sortMergeCoGroup(keyClass, a, b, c, targetParallelism)

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] =
    SMBMultiJoin(self).sortMergeCoGroup(keyClass, a, b, c)

  /** Secondary keyed variant */
  @experimental
  def sortMergeCoGroup[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B], Iterable[C]))] = self.requireNotClosed {
    val tfName = self.tfName
    val keyed = if (self.isTest) {
      SMBMultiJoin(self).testCoGroup(a, b, c)
    } else {
      val t = SortedBucketIO
        .read(keyClass, keyClassSecondary)
        .of(a)
        .and(b)
        .and(c)
        .withTargetParallelism(targetParallelism)
      self.wrap(self.pipeline.apply(s"SMB CoGroupForKey@$tfName", t))
    }

    keyed
      .withName(tfName)
      .map { kv =>
        val k = kv.getKey
        val k1 = k.getKey
        val k2 = k.getValue
        val cgbkResult = kv.getValue
        val asForK = cgbkResult.getAll(a.getTupleTag).asScala
        val bsForK = cgbkResult.getAll(b.getTupleTag).asScala
        val csForK = cgbkResult.getAll(c.getTupleTag).asScala

        (k1, k2) -> ((asForK, bsForK, csForK))
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
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] =
    SMBMultiJoin(self).sortMergeCoGroup(keyClass, a, b, c, d, targetParallelism)

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] =
    SMBMultiJoin(self).sortMergeCoGroup(keyClass, a, b, c, d)

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
  ): SCollection[((K1, K2), (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] =
    self.requireNotClosed {
      val tfName = self.tfName
      val keyed = if (self.isTest) {
        SMBMultiJoin(self).testCoGroup(a, b, c, d)
      } else {
        val t = SortedBucketIO
          .read(keyClass, keyClassSecondary)
          .of(a)
          .and(b)
          .and(c)
          .and(d)
          .withTargetParallelism(targetParallelism)

        self
          .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", t))
      }

      keyed
        .withName(tfName)
        .map { kv =>
          val k = kv.getKey
          val k1 = k.getKey
          val k2 = k.getValue
          val cgbkResult = kv.getValue
          val asForK = cgbkResult.getAll(a.getTupleTag).asScala
          val bsForK = cgbkResult.getAll(b.getTupleTag).asScala
          val csForK = cgbkResult.getAll(c.getTupleTag).asScala
          val dsForK = cgbkResult.getAll(d.getTupleTag).asScala

          (k1, k2) -> ((asForK, bsForK, csForK, dsForK))
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
  ): SortMergeTransform.ReadBuilder[K, K, Void, Iterable[R]] = self.requireNotClosed {
    if (self.isTest) {
      new SortMergeTransform.ReadBuilderTest(self, testCoGroupByKey(read))
    } else {
      val tupleTag = read.getTupleTag

      new SortMergeTransform.ReadBuilderImpl(
        self,
        SortedBucketIO.read(keyClass).of(read).withTargetParallelism(targetParallelism),
        _.getAll(tupleTag).asScala
      )
    }
  }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K, R](
    keyClass: Class[K],
    read: SortedBucketIO.Read[R]
  ): SortMergeTransform.ReadBuilder[K, K, Void, Iterable[R]] =
    sortMergeTransform(keyClass, read, TargetParallelism.auto())

  /** Secondary keyed variant */
  @experimental
  def sortMergeTransform[K1: Coder, K2: Coder, R: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    read: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism
  ): SortMergeTransform.ReadBuilder[KV[K1, K2], K1, K2, Iterable[R]] = self.requireNotClosed {
    val tupleTag = read.getTupleTag
    val fromResult = (result: CoGbkResult) => result.getAll(tupleTag).asScala
    if (self.isTest) {
      val result = SMBMultiJoin(self).testCoGroup[(K1, K2)](read)
      val keyed = result.map { kv =>
        val (k1, k2) = kv.getKey
        KV.of(k1, k2) -> fromResult(kv.getValue)
      }
      new SortMergeTransform.ReadBuilderTest(self, keyed)
    } else {
      val t = SortedBucketIO
        .read(keyClass, keyClassSecondary)
        .of(read)
        .withTargetParallelism(targetParallelism)
      new SortMergeTransform.ReadBuilderImpl(self, t, fromResult)
    }
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
  def sortMergeTransform[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism
  ): SortMergeTransform.ReadBuilder[K, K, Void, (Iterable[A], Iterable[B])] =
    SMBMultiJoin(self).sortMergeTransform(keyClass, a, b, targetParallelism)

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SortMergeTransform.ReadBuilder[K, K, Void, (Iterable[A], Iterable[B])] =
    SMBMultiJoin(self).sortMergeTransform(keyClass, a, b)

  /** Secondary keyed variant */
  @experimental
  def sortMergeTransform[K1: Coder, K2: Coder, A: Coder, B: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    targetParallelism: TargetParallelism
  ): SortMergeTransform.ReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B])] =
    self.requireNotClosed {
      val tupleTagA = a.getTupleTag
      val tupleTagB = b.getTupleTag
      val fromResult = { (result: CoGbkResult) =>
        (
          result.getAll(tupleTagA).asScala,
          result.getAll(tupleTagB).asScala
        )
      }
      if (self.isTest) {
        val result = SMBMultiJoin(self).testCoGroup[(K1, K2)](a, b)
        val keyed = result.map { kv =>
          val (k1, k2) = kv.getKey
          KV.of(k1, k2) -> fromResult(kv.getValue)
        }
        new SortMergeTransform.ReadBuilderTest(self, keyed)
      } else {
        val transform = SortedBucketIO
          .read(keyClass, keyClassSecondary)
          .of(a)
          .and(b)
          .withTargetParallelism(targetParallelism)
        new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
      }
    }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K1: Coder, K2: Coder, A: Coder, B: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SortMergeTransform.ReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B])] =
    sortMergeTransform(keyClass, keyClassSecondary, a, b)

  /**
   * Perform a 3-way [[SortedBucketScioContext.sortMergeCoGroup]] operation, then immediately apply
   * a transformation function to the merged cogroups and re-write using the same bucketing key and
   * hashing scheme. By applying the write, transform, and write in the same transform, an extra
   * shuffle step can be avoided.
   *
   * @group cogroup
   */
  @experimental
  def sortMergeTransform[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism
  ): SortMergeTransform.ReadBuilder[K, K, Void, (Iterable[A], Iterable[B], Iterable[C])] =
    SMBMultiJoin(self).sortMergeTransform(keyClass, a, b, c, targetParallelism)

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SortMergeTransform.ReadBuilder[K, K, Void, (Iterable[A], Iterable[B], Iterable[C])] =
    SMBMultiJoin(self).sortMergeTransform(keyClass, a, b, c)

  /** Secondary keyed variant */
  @experimental
  def sortMergeTransform[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism
  ): SortMergeTransform.ReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B], Iterable[C])] =
    self.requireNotClosed {
      val tupleTagA = a.getTupleTag
      val tupleTagB = b.getTupleTag
      val tupleTagC = c.getTupleTag
      val fromResult = { (result: CoGbkResult) =>
        (
          result.getAll(tupleTagA).asScala,
          result.getAll(tupleTagB).asScala,
          result.getAll(tupleTagC).asScala
        )
      }
      if (self.isTest) {
        val result = SMBMultiJoin(self).testCoGroup[(K1, K2)](a, b, c)
        val keyed = result.map { kv =>
          val (k1, k2) = kv.getKey
          KV.of(k1, k2) -> fromResult(kv.getValue)
        }
        new SortMergeTransform.ReadBuilderTest(self, keyed)
      } else {
        val transform = SortedBucketIO
          .read(keyClass, keyClassSecondary)
          .of(a)
          .and(b)
          .and(c)
          .withTargetParallelism(targetParallelism)
        new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
      }
    }

  /** `targetParallelism` defaults to `TargetParallelism.auto()` */
  @experimental
  def sortMergeTransform[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K1],
    keyClassSecondary: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SortMergeTransform.ReadBuilder[KV[K1, K2], K1, K2, (Iterable[A], Iterable[B], Iterable[C])] =
    sortMergeTransform(
      keyClass,
      keyClassSecondary,
      a,
      b,
      c,
      TargetParallelism.auto()
    )

  @deprecated("Use SortMergeTransform.ReadBuilder instead", "0.14.0")
  type SortMergeTransformReadBuilder[KeyType, K1, K2, R] =
    SortMergeTransform.ReadBuilder[KeyType, K1, K2, R]
  @deprecated("Use SortMergeTransform.WriteBuilder instead", "0.14.0")
  type SortMergeTransformWriteBuilder[KeyType, R, W] =
    SortMergeTransform.WriteBuilder[KeyType, R, W]
  @deprecated("Use SortMergeTransform.WithSideInputsWriteBuilder instead", "0.14.0")
  type SortMergeTransformWithSideInputsWriteBuilder[KeyType, R, W] =
    SortMergeTransform.WithSideInputsWriteBuilder[KeyType, R, W]
}
