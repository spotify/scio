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

import java.util.concurrent.{Callable, CompletableFuture, Executors, ThreadPoolExecutor}
import java.util.function.Supplier

import com.google.common.util.concurrent.{ListenableFuture, MoreExecutors}
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.testing._
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.transforms.{DoFn, ParDo}

import scala.concurrent.{ExecutionContext, Future}

class AsyncDoFnTest extends PipelineSpec {
  private val inputs = Seq(1, 10, 100).map(n => 1 to n)

  private def testDoFn(doFn: DoFn[Int, String], inputs: Seq[Seq[Int]]): Unit =
    for (input <- inputs) {
      runWithContext { sc =>
        val p = sc.parallelize(input).applyTransform(ParDo.of(doFn))
        p should containInAnyOrder(input.map("output-" + _))
      }
    }

  private def testFailure(doFn: DoFn[Int, String]): Unit = {
    val e = the[PipelineExecutionException] thrownBy {
      runWithContext {
        _.parallelize(Seq(1, 2, -1, -2)).applyTransform(ParDo.of(doFn))
      }
    }

    def errorMessages(t: Throwable): List[String] =
      if (t == null) {
        Nil
      } else {
        t.getMessage +: (errorMessages(t.getCause) ++ t.getSuppressed.flatMap(errorMessages))
      }

    errorMessages(e) should contain("Failed to process futures")
    errorMessages(e) should contain("requirement failed: input must be >= 0")
    ()
  }

  "GuavaAsyncDoFn" should "work" in {
    testDoFn(new GuavaDoFn(10), inputs)
  }

  it should "handle failures" in {
    testFailure(new GuavaDoFn(10))
  }

  "JavaAsyncDoFn" should "work" in {
    testDoFn(new JavaDoFn(10), inputs)
  }

  it should "handle failures" in {
    testFailure(new JavaDoFn(10))
  }

  "ScalaAsyncDoFn" should "work" in {
    testDoFn(new ScalaDoFn(10), inputs)
  }

  it should "handle failures" in {
    testFailure(new ScalaDoFn(10))
  }
}

private object Client {
  def process(input: Int): String = {
    require(input >= 0, "input must be >= 0")
    Thread.sleep(input * 10L)
    "output-" + input
  }
}

private class GuavaClient(val numThreads: Int) {
  private val es = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService(
      Executors
        .newFixedThreadPool(numThreads)
        .asInstanceOf[ThreadPoolExecutor]
    )
  )
  def request(input: Int): ListenableFuture[String] =
    es.submit(new Callable[String] {
      override def call(): String = Client.process(input)
    })
}

private class JavaClient(val numThreads: Int) {
  private val es = MoreExecutors.getExitingExecutorService(
    Executors.newFixedThreadPool(numThreads).asInstanceOf[ThreadPoolExecutor]
  )
  def request(input: Int): CompletableFuture[String] =
    CompletableFuture.supplyAsync(
      new Supplier[String] {
        override def get(): String = Client.process(input)
      },
      es
    )
}

private class ScalaClient(val numThreads: Int) {
  private val es = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numThreads))
  def request(input: Int): Future[String] = Future(Client.process(input))(es)
}

private class GuavaDoFn(val numThreads: Int) extends GuavaAsyncDoFn[Int, String, GuavaClient] {
  override def getResourceType: ResourceType = ResourceType.PER_CLASS
  override def createResource(): GuavaClient = new GuavaClient(numThreads)
  override def processElement(input: Int): ListenableFuture[String] =
    getResource.request(input)
}

private class JavaDoFn(val numThreads: Int) extends JavaAsyncDoFn[Int, String, JavaClient] {
  override def getResourceType: ResourceType = ResourceType.PER_CLASS
  override def createResource(): JavaClient = new JavaClient(numThreads)
  override def processElement(input: Int): CompletableFuture[String] =
    getResource.request(input)
}

private class ScalaDoFn(val numThreads: Int) extends ScalaAsyncDoFn[Int, String, ScalaClient] {
  override def getResourceType: ResourceType = ResourceType.PER_CLASS
  override def createResource(): ScalaClient = new ScalaClient(numThreads)
  override def processElement(input: Int): Future[String] =
    getResource.request(input)
}
