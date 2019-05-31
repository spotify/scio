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

package com.spotify.scio.transforms

import java.util.UUID

import com.google.common.collect.{Maps, Queues}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, Setup, StartBundle}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.joda.time.Instant
import com.google.common.base.Preconditions
import com.spotify.scio.transforms.AsyncLookupDoFn.CacheSupplier
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object ScalaAsyncLookupDoFn {
  implicit private class FutureExtension[A](val future: Future[A]) {

    /** Similar to [[future.transform]] in 2.12 to work with Scala 2.11 */
    def transformExtension[B](f: Try[A] => Try[B]): Future[B] = transform(future)(f)
  }

  private def transform[A, B](future: Future[A])(f: Try[A] => Try[B]): Future[B] = {
    val p = Promise[B]()
    future.onComplete { result =>
      try p.complete(f(result))
      catch {
        case NonFatal(ex) => p.failure(ex)
      }
    }
    p.future
  }
}

abstract class ScalaAsyncLookupDoFn[A, B, C](
  maxPendingRequests: Int = 1000,
  cacheSupplier: CacheSupplier[A, B, _] = new AsyncLookupDoFn.NoOpCacheSupplier[A, B]
) extends AsyncDoFn[A, B, C, Try[B], Future[B]](maxPendingRequests, cacheSupplier) {
  private val Log = LoggerFactory.getLogger(classOf[ScalaAsyncLookupDoFn[_, _, _]])

  private val Futures = Maps.newConcurrentMap[UUID, Future[Result]]
  private val Results = Queues.newConcurrentLinkedQueue[Result]

  @Setup override def setup(): Unit = {
    super.setup()
    ()
  }

  @StartBundle override def startBundle(): Unit = {
    super.startBundle()
    Futures.clear()
    Results.clear()
  }

  @ProcessElement def processElement(
    c: DoFn[A, KV[A, Try[B]]]#ProcessContext,
    window: BoundedWindow
  ): Unit = {
    flush((r: Result) => c.output(KV.of(r.input, r.output)))
    val input: A = c.element
    val cached: B = cacheSupplier.get(instanceId, input)
    if (cached != null) {
      c.output(KV.of(input, Try[B](cached)))
    } else {
      val uuid: UUID = UUID.randomUUID
      var future: Future[B] = null
      try {
        semaphore.acquire
        import AsyncDoFn._
        try future = asyncLookup(client.get(instanceId).asInstanceOf[C], input)
        catch {
          case e: Exception =>
            semaphore.release
            throw e
        }
      } catch {
        case e: InterruptedException =>
          Log.error("Failed to acquire semaphore", e)
          throw new RuntimeException("Failed to acquire semaphore", e)
      }
      requestCount += 1
      import ScalaAsyncLookupDoFn._
      val f = future.transformExtension {
        case Success(res) => transformResult(input, Success(res), c.timestamp, window, uuid)
        case Failure(ex) =>
          transformResult(input, Failure(ex), c.timestamp, window, uuid)
      }
      Futures.put(uuid, f)
      ()
    }
  }

  def transformResult(
    input: A,
    res: Try[B],
    timestamp: Instant,
    window: BoundedWindow,
    uuid: UUID
  ): Try[Result] = {
    semaphore.release()
    if (res.isSuccess) {
      cacheSupplier.put(instanceId, input, res.get)
    }
    val r = Result(input, res, timestamp, window)
    Results.add(r)
    Futures.remove(uuid)
    // Return Success for both success and failure because we are call Await.result in finishBundle
    // which assumes that a future is successful.
    Success(r)
  }

  @FinishBundle def finishBundle(c: DoFn[A, KV[A, Try[B]]]#FinishBundleContext): Unit = {
    if (!Futures.isEmpty) {
      val future = Future.sequence(Futures.values.asScala)
      Await.result(future, Duration.Inf)
    }

    flush((r: Result) => c.output(KV.of(r.input, r.output), r.timestamp, r.window))

    // Make sure all requests are processed
    Preconditions.checkState(requestCount == resultCount)
  }

  // Flush pending errors and results
  private def flush(outputFn: Result => Unit): Unit = {
    var r = Results.poll
    while (r != null) {
      outputFn.apply(r)
      resultCount += 1
      r = Results.poll
    }
  }

  case class Result(input: A, output: Try[B], timestamp: Instant, window: BoundedWindow)
}
