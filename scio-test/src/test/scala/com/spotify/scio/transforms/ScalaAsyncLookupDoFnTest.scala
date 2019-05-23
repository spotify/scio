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

import java.util.concurrent.ConcurrentLinkedQueue

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.Futures
import com.spotify.scio.testing.PipelineSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class ScalaAsyncLookupDoFnTest extends PipelineSpec {

  "ScalaAsyncLookupDoFn" should "work" in {
    val fn = new TestScalaAsyncLookupDoFn
    val output = runWithData(1 to 10)(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get))
    output should contain theSameElementsAs (1 to 10).map(x => (x, x.toString))
  }

  it should "work with cache" in {
    val fn = new TestCachingScalaAsyncLookupDoFn
    val output = runWithData((1 to 10) ++ (6 to 15))(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get))
    output should contain theSameElementsAs ((1 to 10) ++ (6 to 15)).map(x => (x, x.toString))
    ScalaAsyncLookupDoFnTest.queue.asScala.toSet should contain theSameElementsAs (1 to 15)
    ScalaAsyncLookupDoFnTest.queue.size() should be <= 20
  }

  it should "work with failures" in {
    val fn = new TestFailingScalaAsyncLookupDoFn
    val output = runWithData(1 to 10)(_.parDo(fn)).map { kv =>
      val r = kv.getValue match {
        case Success(v) => v
        case Failure(e) => e.getMessage
      }
      (kv.getKey, r)
    }
    output should contain theSameElementsAs (1 to 10).map { x =>
      val prefix = if (x % 2 == 0) "success" else "failure"
      (x, prefix + x.toString)
    }
  }

  it should "work with ListenableFuture" in {
    val fn = new TestScalaAsyncLookupListenableFutureDoFn
    val output = runWithData(1 to 10)(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get))
    output should contain theSameElementsAs (1 to 10).map(x => (x, x.toString))
  }

}

object ScalaAsyncLookupDoFnTest {
  val queue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()
}

class TestAsyncClient {}

class TestScalaAsyncLookupDoFn extends ScalaAsyncLookupDoFn[Int, String, TestAsyncClient](10) {
  override def client(): TestAsyncClient = new TestAsyncClient

  override def asyncLookup(client: TestAsyncClient, input: Int): Future[String] =
    Future {
      input.toString
    }
}

class TestCachingScalaAsyncLookupDoFn
    extends ScalaAsyncLookupDoFn[Int, String, TestAsyncClient](10, new TestGuavaCacheSupplier) {

  override def client(): TestAsyncClient = new TestAsyncClient

  override def asyncLookup(client: TestAsyncClient, input: Int): Future[String] = {
    ScalaAsyncLookupDoFnTest.queue.add(input)
    Future {
      input.toString
    }
  }
}

class TestFailingScalaAsyncLookupDoFn
    extends ScalaAsyncLookupDoFn[Int, String, TestAsyncClient](1) {
  override def client(): TestAsyncClient = new TestAsyncClient

  override def asyncLookup(client: TestAsyncClient, input: Int): Future[String] =
    if (input % 2 == 0) {
      Future {
        "success" + input
      }
    } else {
      Future.failed(new RuntimeException("failure" + input))
    }
}

class TestScalaAsyncLookupListenableFutureDoFn
    extends ScalaAsyncLookupDoFn[Int, String, TestAsyncClient](10) {
  override def client(): TestAsyncClient = new TestAsyncClient

  override def asyncLookup(client: TestAsyncClient, input: Int): Future[String] = {
    import ScalaAsyncLookupDoFn._
    Futures.immediateFuture(input.toString)
  }
}

class TestGuavaCacheSupplier extends GuavaCacheSupplier[Int, String, java.lang.Long] {
  override def createCache: Cache[java.lang.Long, String] =
    CacheBuilder.newBuilder().build[java.lang.Long, String]()

  override def getKey(input: Int): java.lang.Long = input.toLong
}
