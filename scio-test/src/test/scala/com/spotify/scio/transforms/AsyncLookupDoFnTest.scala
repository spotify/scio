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

import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue}
import java.util.function.Supplier

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing._
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier
import org.apache.beam.sdk.coders.SerializableCoder

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AsyncLookupDoFnTest extends PipelineSpec {

  // FIXME: KryoAtomicCoder trips up mutation detector by adding stacktrace
  implicit val tryCoder: Coder[BaseAsyncLookupDoFn.Try[String]] =
    Coder.beam(SerializableCoder.of(classOf[BaseAsyncLookupDoFn.Try[String]]))

  private def testDoFn[F, T: Coder](
    doFn: BaseAsyncLookupDoFn[Int, String, AsyncClient, F, T]
  )(tryFn: T => String): Unit = {
    val output = runWithData(1 to 10)(_.parDo(doFn))
      .map(kv => (kv.getKey, tryFn(kv.getValue)))
    output should contain theSameElementsAs (1 to 10).map(x => (x, x.toString))
    ()
  }

  private def testCache[F, T: Coder](
    doFn: BaseAsyncLookupDoFn[Int, String, AsyncClient, F, T]
  )(tryFn: T => String)(queue: ConcurrentLinkedQueue[Int]): Unit = {
    val output = runWithData((1 to 10) ++ (6 to 15))(_.parDo(doFn))
      .map(kv => (kv.getKey, tryFn(kv.getValue)))
    output should contain theSameElementsAs ((1 to 10) ++ (6 to 15)).map(x => (x, x.toString))
    queue.asScala.toSet should contain theSameElementsAs (1 to 15)
    queue.size() should be <= 20
    ()
  }

  private def testFailure[F, T: Coder](
    doFn: BaseAsyncLookupDoFn[Int, String, AsyncClient, F, T]
  )(tryFn: T => Try[String]): Unit = {
    val output = runWithData(1 to 10)(_.parDo(doFn)).map { kv =>
      val r = tryFn(kv.getValue) match {
        case Success(v) => v
        case Failure(e) => e.getMessage
      }
      (kv.getKey, r)
    }
    output should contain theSameElementsAs (1 to 10).map { x =>
      val prefix = if (x % 2 == 0) "success" else "failure"
      (x, prefix + x.toString)
    }
    ()
  }

  "GuavaAsyncLookupDoFn" should "work" in {
    testDoFn(new GuavaLookupDoFn)(_.get())
  }

  it should "work with cache" in {
    testCache(new CachingGuavaLookupDoFn)(_.get())(AsyncLookupDoFnTest.guavaQueue)
  }

  it should "work with failures" in {
    testFailure(new FailingGuavaLookupDoFn)(_.asScala)
  }

  "JavaAsyncLookupDoFn" should "work" in {
    testDoFn(new JavaLookupDoFn)(_.get())
  }

  it should "work with cache" in {
    testCache(new CachingJavaLookupDoFn)(_.get())(AsyncLookupDoFnTest.javaQueue)
  }

  it should "work with failures" in {
    testFailure(new FailingJavaLookupDoFn)(_.asScala)
  }

  "ScalaAsyncLookupDoFn" should "work" in {
    testDoFn(new ScalaLookupDoFn)(_.get)
  }

  it should "work with cache" in {
    testCache(new CachingScalaLookupDoFn)(_.get)(AsyncLookupDoFnTest.scalaQueue)
  }

  it should "work with failures" in {
    testFailure(new FailingScalaLookupDoFn)(identity)
  }
}

object AsyncLookupDoFnTest {
  val guavaQueue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()
  val javaQueue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()
  val scalaQueue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()
}

class AsyncClient {}

class GuavaLookupDoFn extends GuavaAsyncLookupDoFn[Int, String, AsyncClient]() {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): ListenableFuture[String] =
    Futures.immediateFuture(input.toString)
}

class CachingGuavaLookupDoFn
    extends GuavaAsyncLookupDoFn[Int, String, AsyncClient](100, new TestCacheSupplier) {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): ListenableFuture[String] = {
    AsyncLookupDoFnTest.guavaQueue.add(input)
    Futures.immediateFuture(input.toString)
  }
}

class FailingGuavaLookupDoFn extends GuavaAsyncLookupDoFn[Int, String, AsyncClient]() {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): ListenableFuture[String] =
    if (input % 2 == 0) {
      Futures.immediateFuture("success" + input)
    } else {
      Futures.immediateFailedFuture(new RuntimeException("failure" + input))
    }
}

class JavaLookupDoFn extends JavaAsyncLookupDoFn[Int, String, AsyncClient]() {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): CompletableFuture[String] =
    CompletableFuture.supplyAsync(new Supplier[String] {
      override def get(): String = input.toString
    })
}

class CachingJavaLookupDoFn
    extends JavaAsyncLookupDoFn[Int, String, AsyncClient](100, new TestCacheSupplier) {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): CompletableFuture[String] = {
    AsyncLookupDoFnTest.javaQueue.add(input)
    CompletableFuture.supplyAsync(new Supplier[String] {
      override def get(): String = input.toString
    })
  }
}

class FailingJavaLookupDoFn extends JavaAsyncLookupDoFn[Int, String, AsyncClient]() {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): CompletableFuture[String] =
    if (input % 2 == 0) {
      CompletableFuture.supplyAsync(new Supplier[String] {
        override def get(): String = "success" + input
      })
    } else {
      val f = new CompletableFuture[String]()
      f.completeExceptionally(new RuntimeException("failure" + input))
      f
    }
}

class ScalaLookupDoFn extends ScalaAsyncLookupDoFn[Int, String, AsyncClient]() {
  import scala.concurrent.ExecutionContext.Implicits.global
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): Future[String] =
    Future(input.toString)
}

class CachingScalaLookupDoFn
    extends ScalaAsyncLookupDoFn[Int, String, AsyncClient](100, new TestCacheSupplier) {
  import scala.concurrent.ExecutionContext.Implicits.global
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): Future[String] = {
    AsyncLookupDoFnTest.scalaQueue.add(input)
    Future(input.toString)
  }
}

class FailingScalaLookupDoFn extends ScalaAsyncLookupDoFn[Int, String, AsyncClient]() {
  import scala.concurrent.ExecutionContext.Implicits.global
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): Future[String] = {
    if (input % 2 == 0) {
      Future("success" + input)
    } else {
      Future(throw new RuntimeException("failure" + input))
    }
  }
}

class TestCacheSupplier extends CacheSupplier[Int, String, java.lang.Long] {
  override def createCache(): Cache[java.lang.Long, String] =
    CacheBuilder.newBuilder().build[java.lang.Long, String]()
  override def getKey(input: Int): java.lang.Long = input.toLong
}
