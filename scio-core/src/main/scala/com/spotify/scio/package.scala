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

package com.spotify

import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode
import com.spotify.scio.io.Tap
import com.spotify.scio.values.{AccumulatorType, DoubleAccumulatorType, IntAccumulatorType, LongAccumulatorType}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Main package for public APIs. Import all.
 *
 * {{{
 * import com.spotify.scio._
 * }}}
 */
package object scio {

  /** Alias for BigQuery CreateDisposition. */
  val CREATE_IF_NEEDED = Write.CreateDisposition.CREATE_IF_NEEDED

  /** Alias for BigQuery CreateDisposition. */
  val CREATE_NEVER = Write.CreateDisposition.CREATE_NEVER

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_APPEND = Write.WriteDisposition.WRITE_APPEND

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_EMPTY = Write.WriteDisposition.WRITE_EMPTY

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_TRUNCATE = Write.WriteDisposition.WRITE_TRUNCATE

  /** Alias for WindowingStrategy AccumulationMode.ACCUMULATING_FIRED_PANES. */
  val ACCUMULATING_FIRED_PANES = AccumulationMode.ACCUMULATING_FIRED_PANES

  /** Alias for WindowingStrategy AccumulationMode.DISCARDING_FIRED_PANES. */
  val DISCARDING_FIRED_PANES = AccumulationMode.DISCARDING_FIRED_PANES

  implicit val intAccumulatorType: AccumulatorType[Int] = new IntAccumulatorType
  implicit val longAccumulatorType: AccumulatorType[Long] = new LongAccumulatorType
  implicit val doubleAccumulatorType: AccumulatorType[Double] = new DoubleAccumulatorType

  /** Wait for Tap to be available and get Tap reference from Future. */
  implicit class WaitableFutureTap[T](self: Future[Tap[T]]) {
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] = Await.result(self, atMost)
  }

  /** Wait for nested Tap to be available, flatten result and get Tap reference from Future. */
  implicit class WaitableNestedFutureTap[T](self: Future[Future[Tap[T]]]) {
    import scala.concurrent.ExecutionContext.Implicits.global
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] = Await.result(self.flatMap(identity), atMost)
  }

}
