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
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.spotify.scio.testing._
import com.spotify.scio.transforms.AsyncLookupDoFn.CacheSupplier

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class AsyncLookupDoFnTest extends PipelineSpec {

  "AsyncLookupDoFn" should "work" in {
    val fn = new TestAsyncLookupDoFn
    val output = runWithData(1 to 10)(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get()))
    output should contain theSameElementsAs (1 to 10).map(x => (x, x.toString))
  }

  it should "work with cache" in {
    val fn = new TestCachingAsyncLookupDoFn
    val output = runWithData((1 to 10) ++ (6 to 15))(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.asScala.get))
    output should contain theSameElementsAs ((1 to 10) ++ (6 to 15)).map(x => (x, x.toString))
    AsyncLookupDoFnTest.queue.asScala.toSet should contain theSameElementsAs (1 to 15)
    AsyncLookupDoFnTest.queue.size() should be <= 20
  }

  it should "work with failures" in {
    val fn = new TestFailingAsyncLookupDoFn
    val output = runWithData(1 to 10)(_.parDo(fn)).map { kv =>
      val r = kv.getValue.asScala match {
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
}

object AsyncLookupDoFnTest {
  val queue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()
}

class AsyncClient {}

class TestAsyncLookupDoFn extends AsyncLookupDoFn[Int, String, AsyncClient]() {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): ListenableFuture[String] =
    Futures.immediateFuture(input.toString)
}

class TestCachingAsyncLookupDoFn
    extends AsyncLookupDoFn[Int, String, AsyncClient](100, new TestCacheSupplier) {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): ListenableFuture[String] = {
    AsyncLookupDoFnTest.queue.add(input)
    Futures.immediateFuture(input.toString)
  }
}

class TestFailingAsyncLookupDoFn extends AsyncLookupDoFn[Int, String, AsyncClient]() {
  override protected def newClient(): AsyncClient = null
  override def asyncLookup(session: AsyncClient, input: Int): ListenableFuture[String] =
    if (input % 2 == 0) {
      Futures.immediateFuture("success" + input)
    } else {
      Futures.immediateFailedFuture(new RuntimeException("failure" + input))
    }
}

class TestCacheSupplier extends CacheSupplier[Int, String, java.lang.Long] {
  override def createCache(): Cache[java.lang.Long, String] =
    CacheBuilder.newBuilder().build[java.lang.Long, String]()
  override def getKey(input: Int): java.lang.Long = input.toLong
}
