/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.transforms

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, GlobalWindow, IntervalWindow}
import org.apache.beam.sdk.values.TupleTag
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class BatchDoFnTest extends AnyFlatSpec with Matchers {

  class TestReceiver[T] extends OutputReceiver[java.lang.Iterable[T]] {

    private val builder = List.newBuilder[(Iterable[T], Instant)]

    override def output(output: java.lang.Iterable[T]): Unit =
      builder += ((output.asScala, Instant.now()))
    override def outputWithTimestamp(output: java.lang.Iterable[T], timestamp: Instant): Unit =
      builder += ((output.asScala, timestamp))

    def values: List[Iterable[T]] = builder.result().map(_._1)
    def valuesWithTimestamp: List[(Iterable[T], Instant)] = builder.result()
  }

  def intervalWindow(
    start: Instant = Instant.ofEpochSecond(0),
    lengthSeconds: Long = 10
  ): IntervalWindow =
    new IntervalWindow(start, start.plus(lengthSeconds * 1000))

  "BatchDoFn" should "batch items until wight is reached" in {
    val batchFn = new BatchDoFn[Int](10, _.toLong)
    batchFn.setup()

    val receiver = new TestReceiver[Int]
    batchFn.processElement(1, GlobalWindow.INSTANCE, receiver)
    batchFn.processElement(2, GlobalWindow.INSTANCE, receiver)
    batchFn.processElement(3, GlobalWindow.INSTANCE, receiver)
    receiver.values shouldBe empty
    batchFn.processElement(5, GlobalWindow.INSTANCE, receiver)
    receiver.values should have size 1
    receiver.values.head should contain theSameElementsAs Seq(1, 2, 3, 5)
  }

  it should "batch items per window" in {
    val batchFn = new BatchDoFn[Int](10, _.toLong)
    batchFn.setup()
    val window1 = intervalWindow()
    val window2 = intervalWindow(window1.end())
    val receiver = new TestReceiver[Int]
    batchFn.processElement(1, window1, receiver)
    batchFn.processElement(2, window1, receiver)
    batchFn.processElement(3, window1, receiver)
    batchFn.processElement(5, window2, receiver)
    receiver.values shouldBe empty
    batchFn.processElement(5, window1, receiver)
    receiver.values should have size 1
    receiver.values.head should contain theSameElementsAs Seq(1, 2, 3, 5)
    batchFn.processElement(5, window2, receiver)
    receiver.values should have size 2
    receiver.values(1) should contain theSameElementsAs Seq(5, 5)
  }

  it should "flush all pending buffers on finishBundle" in {
    val batchFn = new BatchDoFn[Int](10, _.toLong)
    batchFn.setup()
    val window1 = intervalWindow()
    val window2 = intervalWindow(window1.end())
    val receiver = new TestReceiver[Int]

    val builder = Map.newBuilder[BoundedWindow, Iterable[Int]]
    val finishContext = new batchFn.FinishBundleContext {
      override def getPipelineOptions: PipelineOptions = ???
      override def output(
        output: java.lang.Iterable[Int],
        timestamp: Instant,
        window: BoundedWindow
      ): Unit = builder += (window -> output.asScala)
      override def output[T](
        tag: TupleTag[T],
        output: T,
        timestamp: Instant,
        window: BoundedWindow
      ): Unit = ???
    }

    batchFn.processElement(1, window1, receiver)
    batchFn.processElement(2, window1, receiver)
    batchFn.processElement(3, window1, receiver)
    batchFn.processElement(5, window2, receiver)
    receiver.values shouldBe empty
    batchFn.finishBundle(finishContext)
    val batches = builder.result()
    batches should have size 2
    batches(window1) should contain theSameElementsAs Seq(1, 2, 3)
    batches(window2) should contain theSameElementsAs Seq(5)
  }

  it should "flush the biggest buffer when too many concurrent windows are opened" in {
    val maxLiveWindows = 5
    val batchFn = new BatchDoFn[Int](10, _.toLong, maxLiveWindows)
    batchFn.setup()
    val windows = (0L until maxLiveWindows)
      .map(Instant.ofEpochSecond)
      .map(i => intervalWindow(i, 1))
    val extraWindow = intervalWindow(Instant.ofEpochSecond(maxLiveWindows), 1)
    val receiver = new TestReceiver[Int]

    windows.foreach(w => batchFn.processElement(1, w, receiver))
    batchFn.processElement(2, windows.head, receiver)
    batchFn.processElement(3, windows.head, receiver)

    receiver.values shouldBe empty
    batchFn.processElement(5, extraWindow, receiver)
    receiver.values should have size 1
    val (values, timestamp) = receiver.valuesWithTimestamp.head
    values should contain theSameElementsAs Seq(1, 2, 3)
    timestamp shouldBe windows.head.maxTimestamp()
  }

}
