/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.transforms

import com.google.common.util.concurrent.{ListenableFuture, SettableFuture}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CompletableFuture, Executor, RejectedExecutionException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class GuavaFutureHandler extends FutureHandlers.Guava[String]

class JavaFutureHandler extends FutureHandlers.Java[String]

class RejectFutureHandler extends FutureHandlers.Guava[String] {
  override def getCallbackExecutor: Executor = _ => throw new RejectedExecutionException("Rejected")
}

class FutureHandlersTest extends AnyFlatSpec with Matchers {

  def futureHandler[F, I <: F](
    handler: FutureHandlers.Base[F, String],
    create: () => I,
    complete: I => String => Unit,
    fail: I => Throwable => Unit,
    cancel: I => Unit,
    access: F => String
  ): Unit = {
    it should "block until all futures complete" in {
      val f1 = create()
      val f2 = create()
      val backgroundTask = Future {
        handler.waitForFutures(List[F](f1, f2).asJava)
      }

      complete(f1)("f1 done")
      backgroundTask.isCompleted shouldBe false
      complete(f2)("f2 done")
      Await.result(backgroundTask, 1.second)
    }

    it should "not fail if any future fails" in {
      val f1 = create()
      val f2 = create()
      val backgroundTask = Future {
        handler.waitForFutures(List[F](f1, f2).asJava)
      }

      fail(f1)(new Exception("f1 failed"))
      backgroundTask.isCompleted shouldBe false
      cancel(f2)
      noException shouldBe thrownBy {
        Await.result(backgroundTask, 1.second)
      }
    }

    it should "should execute onSuccess and propagate original future" in {
      val f = create()
      var result: Try[String] = null
      val chainedFuture = handler.addCallback(
        f,
        { value =>
          result = Success(value)
          null
        },
        { e =>
          result = Failure(e)
          null
        }
      )
      complete(f)("success")
      handler.waitForFutures(List(chainedFuture).asJava)
      result shouldBe Success("success")
      access(chainedFuture) shouldBe "success"
    }

    it should "should execute onFailure and propagate original exception" in {
      val f = create()
      var result: Try[String] = null
      val chainedFuture = handler.addCallback(
        f,
        { value =>
          result = Success(value)
          null
        },
        { e =>
          result = Failure(e)
          null
        }
      )
      val e = new Exception("failed")
      fail(f)(e)
      handler.waitForFutures(List(chainedFuture).asJava)
      result shouldBe Failure(e)
      an[Exception] shouldBe thrownBy(access(chainedFuture))
    }

    it should "should execute onFailure if cancelled" in {
      val f = create()
      var result: Try[String] = null
      val chainedFuture = handler.addCallback(
        f,
        { value =>
          result = Success(value)
          null
        },
        { e =>
          result = Failure(e)
          null
        }
      )
      cancel(f)
      handler.waitForFutures(List(chainedFuture).asJava)
      result shouldBe a[Failure[_]]
      an[Exception] shouldBe thrownBy(access(chainedFuture))
    }

    it should "should execute onSuccess and propagate callback exception" in {
      val f = create()
      var result: Try[String] = null
      val chainedFuture = handler.addCallback(
        f,
        { value =>
          result = Success(value)
          throw new Exception("callback failure")
        },
        { e =>
          result = Failure(e)
          null
        }
      )
      complete(f)("success")
      handler.waitForFutures(List(chainedFuture).asJava)
      result shouldBe Success("success")
      an[Exception] shouldBe thrownBy(access(chainedFuture))
    }

    it should "should execute onFailure and propagate original exception with suppressed callback" in {
      val f = create()
      var result: Try[String] = null
      val chainedFuture = handler.addCallback(
        f,
        { value =>
          result = Success(value)
          null
        },
        { e =>
          result = Failure(e)
          throw new Exception("callback failure")
        }
      )
      val e = new Exception("failure")
      fail(f)(e)
      handler.waitForFutures(List(chainedFuture).asJava)
      result shouldBe Failure(e)
      val ee = the[ExecutionException] thrownBy (access(chainedFuture))
      val cause = ee.getCause
      cause.getMessage shouldBe "failure"
      val expectedSuppressed = (handler, sys.props("java.version")) match {
        case (_: JavaFutureHandler, v) if v.startsWith("1.8") =>
          // java 1.8 is not setting the exception as suppressed
          None
        case _ =>
          Some("callback failure")
      }
      cause.getSuppressed.headOption.map(_.getMessage) shouldBe expectedSuppressed
    }
  }

  "Guava handler" should behave like futureHandler[
    ListenableFuture[String],
    SettableFuture[String]
  ](
    new GuavaFutureHandler,
    SettableFuture.create[String],
    _.set,
    _.setException,
    _.cancel(true),
    _.get()
  )

  it should "complete the returned future with failure if callback is rejected" in {
    val handler = new RejectFutureHandler
    val f = SettableFuture.create[String]()
    var result: Try[String] = null
    val chainedFuture = handler.addCallback(
      f,
      { value =>
        result = Success(value)
        null
      },
      { e =>
        result = Failure(e)
        null
      }
    )
    f.set("success")
    handler.waitForFutures(List(chainedFuture).asJava)
    result shouldBe null // callback is not executed
    an[Exception] shouldBe thrownBy(chainedFuture.get())
  }

  "Java handler" should behave like futureHandler[
    CompletableFuture[String],
    CompletableFuture[String]
  ](
    new JavaFutureHandler,
    () => new CompletableFuture[String](),
    _.complete,
    _.completeExceptionally,
    _.cancel(true),
    _.get()
  )
}
