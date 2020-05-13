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

package com.spotify.scio.bigtable

import java.util.concurrent.ConcurrentLinkedQueue

import com.google.cloud.bigtable.grpc.BigtableSession
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.spotify.scio.testing._
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class BigtableDoFnTest extends PipelineSpec {
  "BigtableDoFn" should "work" in {
    val fn = new TestBigtableDoFn
    val output = runWithData(1 to 10)(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get()))
    output should contain theSameElementsAs (1 to 10).map(x => (x, x.toString))
  }

  it should "work with cache" in {
    val fn = new TestCachingBigtableDoFn
    val output = runWithData((1 to 10) ++ (6 to 15))(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get()))
    output should contain theSameElementsAs ((1 to 10) ++ (6 to 15)).map(x => (x, x.toString))
    BigtableDoFnTest.queue.asScala.toSet should contain theSameElementsAs (1 to 15)
    BigtableDoFnTest.queue.size() should be <= 20
  }

  it should "work with failures" in {
    val fn = new TestFailingBigtableDoFn
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

object BigtableDoFnTest {
  val queue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()
}

class TestBigtableDoFn extends BigtableDoFn[Int, String](null) {
  override def newClient(): BigtableSession = null
  override def asyncLookup(session: BigtableSession, input: Int): ListenableFuture[String] =
    Futures.immediateFuture(input.toString)
}

class TestCachingBigtableDoFn extends BigtableDoFn[Int, String](null, 100, new TestCacheSupplier) {
  override def newClient(): BigtableSession = null
  override def asyncLookup(session: BigtableSession, input: Int): ListenableFuture[String] = {
    BigtableDoFnTest.queue.add(input)
    Futures.immediateFuture(input.toString)
  }
}

class TestFailingBigtableDoFn extends BigtableDoFn[Int, String](null) {
  override def newClient(): BigtableSession = null
  override def asyncLookup(session: BigtableSession, input: Int): ListenableFuture[String] =
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
