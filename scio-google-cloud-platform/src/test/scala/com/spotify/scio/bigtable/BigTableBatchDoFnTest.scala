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

package com.spotify.scio.bigtable

import java.util.concurrent.{CompletionException, ConcurrentLinkedQueue}
import com.google.cloud.bigtable.grpc.BigtableSession
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.spotify.scio.testing._
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier
import com.spotify.scio.transforms.JavaAsyncConverters._
import org.apache.commons.lang3.tuple.Pair

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class BigtableBatchDoFnTest extends PipelineSpec {
  "BigtableDoFn" should "work" in {
    val fn = new TestBigtableBatchDoFn
    val output = runWithData(1 to 10)(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get()))
    output should contain theSameElementsAs (1 to 10).map(x => (x, x.toString))
  }

  it should "work with cache" in {
    val fn = new TestCachingBigtableBatchDoFn
    val output = runWithData((1 to 10) ++ (6 to 15))(_.parDo(fn))
      .map(kv => (kv.getKey, kv.getValue.get()))
    output should have size 20
    output should contain theSameElementsAs ((1 to 10) ++ (6 to 15)).map(x => (x, x.toString))
    BigtableBatchDoFnTest.queue.asScala.toSet should contain theSameElementsAs (1 to 15)
    BigtableBatchDoFnTest.queue.size() should be <= 20
  }

  it should "work with failures" in {
    val fn = new TestFailingBigtableBatchDoFn

    val output = runWithData(Seq[Seq[Int]](1 to 4, 8 to 10))(_.flatten.parDo(fn)).map { kv =>
      val r = kv.getValue.asScala match {
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
}

object BigtableBatchDoFnTest {
  val queue: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]()

  def batchRequest(input: java.util.List[Int]): List[Int] = input.asScala.toList
  def batchResponse(input: List[String]): java.util.List[Pair[String, String]] =
    input.map(x => Pair.of(x, x)).asJava
  def idExtractor(input: Int): String = input.toString
}

class TestBigtableBatchDoFn
    extends BigtableBatchDoFn[Int, List[Int], List[String], String](
      null,
      2,
      BigtableBatchDoFnTest.batchRequest,
      BigtableBatchDoFnTest.batchResponse,
      BigtableBatchDoFnTest.idExtractor
    ) {
  override def newClient(): BigtableSession = null
  override def asyncLookup(
    session: BigtableSession,
    input: List[Int]
  ): ListenableFuture[List[String]] =
    Futures.immediateFuture(input.map(_.toString))
}

class TestCachingBigtableBatchDoFn
    extends BigtableBatchDoFn[Int, List[Int], List[String], String](
      null,
      2,
      BigtableBatchDoFnTest.batchRequest,
      BigtableBatchDoFnTest.batchResponse,
      BigtableBatchDoFnTest.idExtractor,
      100,
      new TestCacheBatchSupplier
    ) {
  override def newClient(): BigtableSession = null

  override def asyncLookup(
    session: BigtableSession,
    input: List[Int]
  ): ListenableFuture[List[String]] = {
    input.foreach(BigtableBatchDoFnTest.queue.add)
    Futures.immediateFuture(input.map(_.toString))
  }
}

class TestFailingBigtableBatchDoFn
    extends BigtableBatchDoFn[Int, List[Int], List[String], String](
      null,
      4,
      BigtableBatchDoFnTest.batchRequest,
      BigtableBatchDoFnTest.batchResponse,
      BigtableBatchDoFnTest.idExtractor
    ) {
  override def newClient(): BigtableSession = null
  override def asyncLookup(
    session: BigtableSession,
    input: List[Int]
  ): ListenableFuture[List[String]] =
    if (input.size % 2 == 0) {
      Futures.immediateFuture(input.map(_.toString))
    } else {
      Futures.immediateFailedFuture(new RuntimeException("failure for " + input.mkString(",")))
    }
}

class TestCacheBatchSupplier extends CacheSupplier[String, String] {
  override def get(): Cache[String, String] = CacheBuilder.newBuilder().build()
}
