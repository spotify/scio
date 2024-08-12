/*
 * Copyright 2024 Spotify AB.
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

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.{Futures, ListenableFuture, MoreExecutors}
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing._
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.{CacheSupplier, NoOpCacheSupplier}
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.transforms.JavaAsyncConverters._
import com.spotify.scio.util.Functions
import org.apache.commons.lang3.tuple.Pair
import org.joda.time.Instant

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent._
import scala.annotation.nowarn
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class AsyncBatchLookupDoFnTest extends PipelineSpec {
  private def testDoFn[F, T: Coder, C <: AsyncBatchClient](
    doFn: BaseAsyncBatchLookupDoFn[Int, List[Int], List[String], String, C, F, T]
  )(tryFn: T => String): Unit = {
    val output = runWithData(Seq[Seq[Int]](1 to 10))(_.flatten.parDo(doFn))
      .map(kv => (kv.getKey, tryFn(kv.getValue)))
    output should contain theSameElementsAs (1 to 10).map(x => (x, x.toString))
    ()
  }

  private def testCache[F, T: Coder](
    doFn: BaseAsyncBatchLookupDoFn[Int, List[Int], List[String], String, AsyncBatchClient, F, T]
  )(tryFn: T => String): Unit = {
    AsyncBatchLookupDoFnTest.queue.clear()
    val output = runWithData(Seq[Seq[Int]](1 to 10, 6 to 15))(_.flatten.parDo(doFn))
      .map(kv => (kv.getKey, tryFn(kv.getValue)))
    output should contain theSameElementsAs ((1 to 10) ++ (6 to 15)).map(x => (x, x.toString))
    AsyncBatchLookupDoFnTest.queue.asScala.toSet should contain theSameElementsAs (1 to 15)
    AsyncBatchLookupDoFnTest.queue.size() should be <= 20
    ()
  }

  private def testFailure[F, T: Coder](
    doFn: BaseAsyncBatchLookupDoFn[Int, List[Int], List[String], String, AsyncBatchClient, F, T]
  )(tryFn: T => Try[String]): Unit = {
    // batches of size 4 and size 3
    val output = runWithData(Seq[Seq[Int]](1 to 4, 8 to 10))(_.flatten.parDo(doFn)).map { kv =>
      val r = tryFn(kv.getValue) match {
        case Success(v)                      => v
        case Failure(e: CompletionException) => e.getCause.getMessage
        case Failure(e)                      => e.getMessage
      }
      (kv.getKey, r)
    }
    output should contain theSameElementsAs (
      (1 to 4).map(x => x -> x.toString) ++
        (8 to 10).map(x => x -> "failure for 8,9,10")
    )
  }

  "BaseAsyncBatchLookupDoFn" should "batch requests" in {
    val n = 100
    val output = runWithData(Seq(Seq.range(0, n))) {
      _.flatten // trick to get a single bundle
        .parDo(new CountingGuavaBatchLookupDoFn)
        .map(_.getValue.get())
        .map(_.toInt)
    }
    output.max shouldBe (n / AsyncBatchLookupDoFnTest.BatchSize)
  }

  it should "close client" in {
    runWithContext(_.parallelize(Seq(1, 10, 100)).parDo(new ClosableAsyncLookupDoFn))
    ClosableResourceCounters.clientsOpened.get() should be > 0
    ClosableResourceCounters.allResourcesClosed shouldBe true
  }

  it should "propagate element metadata" in {
    runWithContext { sc =>
      // try to use a single bundle so we can check
      // elements flushed in processElement as well as
      // elements flushed in finishBundle
      val data = sc
        .parallelize(Seq[Seq[Int]](1 to 10))
        .flatten
        .timestampBy(x => Instant.ofEpochMilli(x.toLong))
        .parDo(new GuavaBatchLookupDoFn)
        .map(kv => (kv.getKey, kv.getValue))
        .withTimestamp
        .map { case ((k, v), ts) => (k, v.get(), ts.getMillis) }
      data should containInAnyOrder((1 to 10).map(x => (x, x.toString, x.toLong)))
    }
  }

  "GuavaAsyncBatchLookupDoFn" should "work" in {
    testDoFn(new GuavaBatchLookupDoFn)(_.get())
  }

  it should "work with cache" in {
    testCache(new CachingGuavaBatchLookupDoFn)(_.get())
  }

  it should "work with failures" in {
    testFailure(new FailingGuavaBatchLookupDoFn)(_.asScala)
  }

  "JavaAsyncBatchLookupDoFn" should "work" in {
    testDoFn(new JavaBatchLookupDoFn)(_.get())
  }

  it should "work with cache" in {
    testCache(new CachingJavaBatchLookupDoFn)(_.get())
  }

  it should "work with failures" in {
    testFailure(new FailingJavaBatchLookupDoFn)(_.asScala)
  }

  "ScalaAsyncBatchLookupDoFn" should "work" in {
    testDoFn(new ScalaBatchLookupDoFn)(_.get)
  }

  it should "work with cache" in {
    testCache(new CachingScalaBatchLookupDoFn)(_.get)
  }

  it should "work with failures" in {
    testFailure(new FailingScalaBatchLookupDoFn)(identity)
  }

  it should "work with failures on callback" in {
    val errors =
      runWithData(1 to 10)(_.parDo(new CallbackFailingScalaLookupDoFn)).filter(_.getValue.isFailure)
    assert(errors.size == 10)
  }
}

object AsyncBatchLookupDoFnTest {
  val queue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()
  val BatchSize = 4
}

trait AsyncBatchClient

/** Returns the count of lookups as the lookup result */
class CountingAsyncBatchClient extends AsyncBatchClient with Serializable {
  @transient private lazy val es = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService(
      Executors
        .newFixedThreadPool(5)
        .asInstanceOf[ThreadPoolExecutor]
    )
  )

  var count: AtomicInteger = new AtomicInteger(0)
  def lookup(input: List[Int]): ListenableFuture[List[String]] = {
    val cnt = count.addAndGet(1)
    es.submit { () =>
      Thread.sleep(1000)
      input.map(x => s"$x:$cnt")
    }
  }
}

abstract class AbstractGuavaAsyncBatchLookupDoFn(
  batchSize: Int = AsyncBatchLookupDoFnTest.BatchSize,
  batchRequestFn: java.util.List[Int] => List[Int] = _.asScala.toList,
  batchResponseFn: List[String] => java.util.List[Pair[String, String]] =
    _.map(x => Pair.of(x, x)).asJava,
  idExtractionFn: Int => String = _.toString,
  cacheSupplier: CacheSupplier[String, String] = new NoOpCacheSupplier[String, String]()
) extends GuavaAsyncBatchLookupDoFn[Int, List[Int], List[String], String, AsyncBatchClient](
      batchSize,
      Functions.serializableFn(batchRequestFn),
      Functions.serializableFn(batchResponseFn),
      Functions.serializableFn(idExtractionFn),
      100,
      cacheSupplier
    )

class GuavaBatchLookupDoFn extends AbstractGuavaAsyncBatchLookupDoFn() {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(
    session: AsyncBatchClient,
    input: List[Int]
  ): ListenableFuture[List[String]] =
    Futures.immediateFuture(input.map(_.toString))
}

class CachingGuavaBatchLookupDoFn
    extends AbstractGuavaAsyncBatchLookupDoFn(
      batchSize = 1,
      cacheSupplier = new TestBatchCacheSupplier
    ) {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(
    session: AsyncBatchClient,
    input: List[Int]
  ): ListenableFuture[List[String]] = {
    input.foreach(AsyncBatchLookupDoFnTest.queue.add)
    Futures.immediateFuture(input.map(_.toString))
  }
}

class FailingGuavaBatchLookupDoFn extends AbstractGuavaAsyncBatchLookupDoFn() {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(
    session: AsyncBatchClient,
    input: List[Int]
  ): ListenableFuture[List[String]] =
    if (input.size % 2 == 0) {
      Futures.immediateFuture(input.map(_.toString))
    } else {
      Futures.immediateFailedFuture(new RuntimeException("failure for " + input.mkString(",")))
    }
}

class CountingGuavaBatchLookupDoFn
    extends AbstractGuavaAsyncBatchLookupDoFn(
      batchResponseFn =
        _.map(_.split(":")).map { case Array(k, v) => Pair.of(k, v) }.asJava: @nowarn
    ) {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = new CountingAsyncBatchClient()
  override def asyncLookup(
    session: AsyncBatchClient,
    input: List[Int]
  ): ListenableFuture[List[String]] =
    session.asInstanceOf[CountingAsyncBatchClient].lookup(input)
}

abstract class AbstractJavaAsyncBatchLookupDoFn(
  batchSize: Int = AsyncBatchLookupDoFnTest.BatchSize,
  batchRequestFn: java.util.List[Int] => List[Int] = _.asScala.toList,
  batchResponseFn: List[String] => java.util.List[Pair[String, String]] =
    _.map(x => Pair.of(x, x)).asJava,
  idExtractionFn: Int => String = _.toString,
  cacheSupplier: CacheSupplier[String, String] = new NoOpCacheSupplier[String, String]()
) extends JavaAsyncBatchLookupDoFn[Int, List[Int], List[String], String, AsyncBatchClient](
      batchSize,
      Functions.serializableFn(batchRequestFn),
      Functions.serializableFn(batchResponseFn),
      Functions.serializableFn(idExtractionFn),
      100,
      cacheSupplier
    )

class JavaBatchLookupDoFn extends AbstractJavaAsyncBatchLookupDoFn() {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(
    session: AsyncBatchClient,
    input: List[Int]
  ): CompletableFuture[List[String]] =
    CompletableFuture.supplyAsync(() => input.map(_.toString))
}

class CachingJavaBatchLookupDoFn
    extends AbstractJavaAsyncBatchLookupDoFn(
      batchSize = 1,
      cacheSupplier = new TestBatchCacheSupplier
    ) {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(
    session: AsyncBatchClient,
    input: List[Int]
  ): CompletableFuture[List[String]] = {
    input.foreach(AsyncBatchLookupDoFnTest.queue.add)
    CompletableFuture.supplyAsync(() => input.map(_.toString))
  }
}

class FailingJavaBatchLookupDoFn extends AbstractJavaAsyncBatchLookupDoFn() {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(
    session: AsyncBatchClient,
    input: List[Int]
  ): CompletableFuture[List[String]] =
    if (input.size % 2 == 0) {
      CompletableFuture.supplyAsync(() => input.map(_.toString))
    } else {
      val f = new CompletableFuture[List[String]]()
      f.completeExceptionally(new RuntimeException("failure for " + input.mkString(",")))
      f
    }
}

abstract class AbstractScalaAsyncBatchLookupDoFn(
  batchSize: Int = AsyncBatchLookupDoFnTest.BatchSize,
  batchRequestFn: Iterable[Int] => List[Int] = _.toList,
  batchResponseFn: List[String] => Iterable[(String, String)] = _.map(x => x -> x),
  idExtractionFn: Int => String = _.toString,
  cacheSupplier: CacheSupplier[String, String] = new NoOpCacheSupplier[String, String]()
) extends ScalaAsyncBatchLookupDoFn[Int, List[Int], List[String], String, AsyncBatchClient](
      batchSize,
      batchRequestFn,
      batchResponseFn,
      idExtractionFn,
      100,
      cacheSupplier
    )

class ScalaBatchLookupDoFn extends AbstractScalaAsyncBatchLookupDoFn() {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(session: AsyncBatchClient, input: List[Int]): Future[List[String]] =
    Future.successful(input.map(_.toString))
}

class CachingScalaBatchLookupDoFn
    extends AbstractScalaAsyncBatchLookupDoFn(
      batchSize = 1,
      cacheSupplier = new TestBatchCacheSupplier
    ) {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(session: AsyncBatchClient, input: List[Int]): Future[List[String]] = {
    input.foreach(AsyncBatchLookupDoFnTest.queue.add)
    Future.successful(input.map(_.toString))
  }
}

class FailingScalaBatchLookupDoFn extends AbstractScalaAsyncBatchLookupDoFn() {
  override def getResourceType: ResourceType = ResourceType.PER_INSTANCE
  override protected def newClient(): AsyncBatchClient = null
  override def asyncLookup(session: AsyncBatchClient, input: List[Int]): Future[List[String]] =
    if (input.size % 2 == 0) {
      Future.successful(input.map(_.toString))
    } else {
      Future.failed(new RuntimeException("failure for " + input.mkString(",")))
    }
}

private class ClosableAsyncBatchLookupDoFn extends ScalaAsyncDoFn[Int, String, CloseableResource] {
  override def getResourceType: ResourceType = ResourceType.PER_CLASS
  override def processElement(input: Int): Future[String] = Future.successful(input.toString)
  override def createResource(): CloseableResource = new CloseableResource {}
}

class TestBatchCacheSupplier extends CacheSupplier[String, String] {
  override def get(): Cache[String, String] = CacheBuilder.newBuilder().build()
}
