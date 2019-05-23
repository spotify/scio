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
import java.util.concurrent.Semaphore
import java.util.function.{Function => JFunction}

import com.google.common.cache.Cache
import com.google.common.collect.{Maps, Queues}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, Setup, StartBundle}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.joda.time.Instant
import com.google.common.base.Preconditions
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn.Caches
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.language.implicitConversions
import scala.util.control.NonFatal

object ScalaAsyncLookupDoFn {
  val Client = Maps.newConcurrentMap[UUID, Any]
  val Caches = Maps.newConcurrentMap[UUID, Cache[_, _]]

  implicit def toJavaFunction[T, U](f: Function1[T, U]): JFunction[T, U] = new JFunction[T, U] {
    override def apply(t: T): U = f(t)
  }

  implicit def toScalaFuture[T](lFuture: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]
    Futures.addCallback(lFuture, new FutureCallback[T] {
      def onSuccess(result: T) = p.success(result)
      def onFailure(t: Throwable) = p.failure(t)
    }, MoreExecutors.directExecutor())
    p.future
  }

  /** Work around for future.transform in 2.11 to be like in 2.12 */
  def transform[A, B](future: Future[A])(f: Try[A] => Try[B]): Future[B] = {
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
  maxPendingRequests: Int,
  cacheSupplier: GuavaCacheSupplier[A, B, _]
) extends DoFn[A, KV[A, Try[B]]] {
  private val Log = LoggerFactory.getLogger(classOf[ScalaAsyncLookupDoFn[_, _, _]])

  private val InstanceId: UUID = UUID.randomUUID
  private val Semaphore = new Semaphore(maxPendingRequests)
  private val Futures = Maps.newConcurrentMap[UUID, Future[Result]]
  private val Results = Queues.newConcurrentLinkedQueue[Result]
  private var RequestCount: Long = 0L
  private var ResultCount: Long = 0L

  def this(maxPendingRequests: Int) {
    this(maxPendingRequests, new NoOpGuavaCacheSupplier[A, B, String])
  }

  /** Perform asynchronous lookup */
  protected def asyncLookup(client: C, input: A): Future[B]

  /** Define client */
  protected def client(): C

  @Setup def setup(): Unit = {
    import ScalaAsyncLookupDoFn._
    val clientFunc = (uuid: UUID) => client
    Client.computeIfAbsent(InstanceId, clientFunc)
    val cacheFunc = (uuid: UUID) => cacheSupplier.createCache
    Caches.computeIfAbsent(InstanceId, cacheFunc)
    ()
  }

  @StartBundle def startBundle(): Unit = {
    Futures.clear()
    Results.clear()
    RequestCount = 0
    ResultCount = 0
  }

  @ProcessElement def processElement(
    c: DoFn[A, KV[A, Try[B]]]#ProcessContext,
    window: BoundedWindow
  ): Unit = {
    flush((r: Result) => c.output(KV.of(r.input, r.output)))
    val input: A = c.element
    val cached: Option[B] = cacheSupplier.get(InstanceId, input)
    if (cached.nonEmpty) {
      c.output(KV.of(input, Try[B](cached.get)))
    } else {
      val uuid: UUID = UUID.randomUUID
      var future: Future[B] = null
      try {
        Semaphore.acquire
        import ScalaAsyncLookupDoFn._
        try future = asyncLookup(Client.get(InstanceId).asInstanceOf[C], input)
        catch {
          case e: Exception =>
            Semaphore.release
            throw e
        }
      } catch {
        case e: InterruptedException =>
          Log.error("Failed to acquire semaphore", e)
          throw new RuntimeException("Failed to acquire semaphore", e)
      }
      RequestCount += 1
      val f = ScalaAsyncLookupDoFn.transform(future) {
        case Success(res) => transform(input, Success(res), c.timestamp, window, uuid)
        case Failure(ex) =>
          transform(input, Failure(ex), c.timestamp, window, uuid)
      }
      Futures.put(uuid, f)
      ()
    }
  }

  def transform(
    input: A,
    res: Try[B],
    timestamp: Instant,
    window: BoundedWindow,
    uuid: UUID
  ): Try[Result] = {
    Semaphore.release()
    if (res.isSuccess) {
      cacheSupplier.put(InstanceId, input, res.get)
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
    Preconditions.checkState(RequestCount == ResultCount)
  }

  // Flush pending errors and results
  private def flush(outputFn: Result => Unit): Unit = {
    var r = Results.poll
    while (r != null) {
      outputFn.apply(r)
      ResultCount += 1
      r = Results.poll
    }
  }

  case class Result(input: A, output: Try[B], timestamp: Instant, window: BoundedWindow)
}

abstract class GuavaCacheSupplier[A, B, K] extends Serializable {

  def createCache: Cache[K, B]

  def getKey(input: A): K

  final def get(instanceId: UUID, item: A): Option[B] = {
    val c: Cache[K, B] = Caches.get(instanceId).asInstanceOf[Cache[K, B]]
    if (c == null) {
      Option.empty
    } else {
      Option(c.getIfPresent(getKey(item)))
    }
  }

  final def put(instanceId: UUID, item: A, value: B): Unit = {
    val c: Cache[K, B] = Caches.get(instanceId).asInstanceOf[Cache[K, B]]
    if (c != null) {
      c.put(getKey(item), value)
    }
  }
}

final class NoOpGuavaCacheSupplier[A, B, _] extends GuavaCacheSupplier[A, B, String] {
  override def createCache: Cache[String, B] = null

  override def getKey(input: A): String = null.asInstanceOf[String]
}
