/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.transforms

import java.lang.{Iterable => JIterable}
import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * A [[org.apache.beam.sdk.transforms.DoFn DoFn]] that handles asynchronous requests to an
 * external service that returns Scala [[Future]]s.
 */
abstract class ScalaAsyncDoFn[I, O, R]
    extends BaseAsyncDoFn[I, O, R, Future[O]] {

  @transient
  private lazy implicit val immediateExecutionContext = new ExecutionContext {
    override def execute(runnable: Runnable): Unit = runnable.run()
    override def reportFailure(cause: Throwable): Unit =
      ExecutionContext.defaultReporter(cause)
  }

  override protected def waitForFutures(futures: JIterable[Future[O]]): Unit =
    Await.ready(Future.sequence(futures.asScala), Duration.Inf)

  override protected def addCallback(
    future: Future[O],
    onSuccess: JFunction[O, Void],
    onFailure: JFunction[Throwable, Void]): Future[O] =
    future.transform(r => { onSuccess(r); r }, t => { onFailure(t); t })

}
