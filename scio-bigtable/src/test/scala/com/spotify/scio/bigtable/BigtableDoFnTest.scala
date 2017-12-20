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

package com.spotify.scio.bigtable

import com.google.cloud.bigtable.grpc.BigtableSession
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.spotify.scio.bigtable.BigtableDoFn.CacheSupplier
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.values.KV
import org.scalatest._

import scala.collection.mutable
import scala.collection.JavaConverters._

class BigtableDoFnTest extends FlatSpec with Matchers {

  "BigtableDoFn" should "work" in {
    val fn = new TestBigtableDoFn
    val output = DoFnTester.of(fn).processBundle((1 to 10).asJava)
      .asScala.map(kv => (kv.getKey, kv.getValue.get()))
    output shouldBe (1 to 10).map(x => (x, x.toString))
  }

  it should "work with cache" in {
    val fn = new TestCachingBigtableDoFn
    val output = DoFnTester.of(fn).processBundle(((1 to 10) ++ (5 to 15)).asJava)
      .asScala.map(kv => (kv.getKey, kv.getValue.get()))
    output shouldBe ((1 to 10) ++ (5 to 15)).map(x => (x, x.toString))
    BigtableDoFnTest.queue shouldBe (1 to 15)
  }

  it should "work with failures" in {
    val fn = new TestFailingBigtableDoFn
    val output = DoFnTester.of(fn).processBundle((1 to 10).asJava).asScala.map { kv =>
      val v = kv.getValue
      (kv.getKey, if (v.isSuccess) v.get() else v.getException.getMessage)
    }
    output shouldBe (1 to 10).map { x =>
      val prefix = if (x % 2 == 0) "success" else "failure"
      (x, prefix + x.toString)
    }
  }
}

object BigtableDoFnTest {
  val queue: mutable.Queue[Int] = mutable.Queue.empty
}

class TestBigtableDoFn extends BigtableDoFn[Int, String](null) {
  override def asyncLookup(session: BigtableSession, input: Int): ListenableFuture[String] =
    Futures.immediateFuture(input.toString)
}

class TestCachingBigtableDoFn extends BigtableDoFn[Int, String](null, 100, new TestCacheSupplier) {
  override def asyncLookup(session: BigtableSession, input: Int): ListenableFuture[String] = {
    BigtableDoFnTest.queue.enqueue(input)
    Futures.immediateFuture(input.toString)
  }
}

class TestFailingBigtableDoFn extends BigtableDoFn[Int, String](null) {
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
