/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.smb

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.values.{SCollection, SideInput, SideInputContext}
import org.apache.beam.sdk.extensions.smb.{SortedBucketIOUtil, SortedBucketTransform}
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.{
  AbsCoGbkTransform,
  TransformOutput,
  Transformable
}
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.{BucketItem, MergedBucket}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement}
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.{KV, PCollectionView}

import scala.jdk.CollectionConverters._

object SortMergeTransform {
  sealed trait ReadBuilder[KeyType, K1, K2, R] extends Serializable {
    def to[W: Coder](
      output: TransformOutput[K1, K2, W]
    ): WriteBuilder[KeyType, R, W]
  }

  private[smb] class ReadBuilderImpl[KeyType, K1, K2, R](
    @transient private val sc: ScioContext,
    coGbk: Transformable[KeyType, K1, K2],
    fromResult: CoGbkResult => R
  ) extends ReadBuilder[KeyType, K1, K2, R] {
    override def to[W: Coder](
      output: TransformOutput[K1, K2, W]
    ): WriteBuilder[KeyType, R, W] =
      new WriteBuilderImpl(sc, coGbk.transform(output), fromResult)
  }

  private[smb] class ReadBuilderTest[KeyType, K1, K2, R](
    @transient private val sc: ScioContext,
    read: => SCollection[(KeyType, R)]
  ) extends ReadBuilder[KeyType, K1, K2, R] {
    override def to[W: Coder](
      output: TransformOutput[K1, K2, W]
    ): WriteBuilder[KeyType, R, W] =
      new WriteBuilderTest(sc, read, output)
  }

  sealed trait WriteBuilder[KeyType, R, W] extends Serializable {
    def withSideInputs(
      sides: SideInput[_]*
    ): WithSideInputsWriteBuilder[KeyType, R, W]

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
    ): ClosedTap[Nothing]
  }

  private[smb] class WriteBuilderImpl[KeyType, R, W](
    @transient private val sc: ScioContext,
    transform: AbsCoGbkTransform[KeyType, W],
    fromResult: CoGbkResult => R
  ) extends WriteBuilder[KeyType, R, W] {
    override def withSideInputs(
      sides: SideInput[_]*
    ): WithSideInputsWriteBuilder[KeyType, R, W] =
      new WithSideInputsWriteBuilderImpl(sc, transform, fromResult, sides)

    override def via(
      transformFn: (KeyType, R, SortedBucketTransform.SerializableConsumer[W]) => Unit
    ): ClosedTap[Nothing] = {
      val fn = new SortedBucketTransform.TransformFn[KeyType, W]() {
        override def writeTransform(
          keyGroup: KV[KeyType, CoGbkResult],
          outputConsumer: SortedBucketTransform.SerializableConsumer[W]
        ): Unit =
          transformFn.apply(
            keyGroup.getKey,
            fromResult(keyGroup.getValue),
            outputConsumer
          )
      }

      val t = transform.via(fn)
      val tfName = sc.tfName(Some("sortMergeTransform"))
      sc.applyInternal(tfName, t)
      ClosedTap[Nothing](EmptyTap)
    }
  }

  private[smb] class WriteBuilderTest[KeyType, K1, K2, R, W: Coder](
    @transient private val sc: ScioContext,
    read: => SCollection[(KeyType, R)],
    output: TransformOutput[K1, K2, W]
  ) extends WriteBuilder[KeyType, R, W] {

    override def withSideInputs(
      sides: SideInput[_]*
    ): WithSideInputsWriteBuilder[KeyType, R, W] =
      new WithSideInputsWriteBuilderTest(sc, read, output)

    override def via(
      transformFn: (KeyType, R, SortedBucketTransform.SerializableConsumer[W]) => Unit
    ): ClosedTap[Nothing] = {
      val data = read.parDo(new ViaTransform(transformFn))
      val testOutput = TestDataManager.getOutput(sc.testId.get)
      testOutput(SortedBucketIOUtil.testId(output))(data)
      ClosedTap[Nothing](EmptyTap)
    }
  }

  private class ViaTransform[KeyType, R, W](
    transformFn: (KeyType, R, SortedBucketTransform.SerializableConsumer[W]) => Unit
  ) extends DoFn[(KeyType, R), W] {
    @ProcessElement
    def processElement(
      @Element element: (KeyType, R),
      outputReceiver: OutputReceiver[W]
    ): Unit = {
      val (key, value) = element
      transformFn(key, value, outputReceiver.output(_))
    }
  }

  sealed trait WithSideInputsWriteBuilder[KeyType, R, W] extends Serializable {
    def via(
      transformFn: (
        KeyType,
        R,
        SideInputContext[_],
        SortedBucketTransform.SerializableConsumer[W]
      ) => Unit
    ): ClosedTap[Nothing]
  }

  private[smb] class WithSideInputsWriteBuilderImpl[KeyType, R, W](
    @transient private val sc: ScioContext,
    transform: AbsCoGbkTransform[KeyType, W],
    toR: CoGbkResult => R,
    sides: Iterable[SideInput[_]]
  ) extends WithSideInputsWriteBuilder[KeyType, R, W] {
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
      sc.applyInternal(t)
      ClosedTap[Nothing](EmptyTap)
    }
  }

  private[smb] class WithSideInputsWriteBuilderTest[KeyType, K1, K2, R, W: Coder](
    @transient private val sc: ScioContext,
    read: => SCollection[(KeyType, R)],
    output: TransformOutput[K1, K2, W]
  ) extends WithSideInputsWriteBuilder[KeyType, R, W] {
    override def via(
      transformFn: (
        KeyType,
        R,
        SideInputContext[_],
        SortedBucketTransform.SerializableConsumer[W]
      ) => Unit
    ): ClosedTap[Nothing] = {
      val data = read.parDo(new ViaTransformWithSideOutput(transformFn))
      val testOutput = TestDataManager.getOutput(sc.testId.get)
      testOutput(SortedBucketIOUtil.testId(output))(data)
      ClosedTap[Nothing](EmptyTap)
    }
  }

  private class ViaTransformWithSideOutput[KeyType, R, W](
    transformFn: (
      KeyType,
      R,
      SideInputContext[_],
      SortedBucketTransform.SerializableConsumer[W]
    ) => Unit
  ) extends DoFn[(KeyType, R), W] {
    @ProcessElement
    def processElement(
      c: DoFn[(KeyType, R), W]#ProcessContext,
      w: BoundedWindow
    ): Unit = {
      val (key, value) = c.element()
      transformFn(
        key,
        value,
        new SideInputContext(c.asInstanceOf[DoFn[BucketItem, AnyRef]#ProcessContext], w),
        c.output(_)
      )
    }
  }

}
