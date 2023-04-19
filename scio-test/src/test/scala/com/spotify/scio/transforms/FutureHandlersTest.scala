package com.spotify.scio.transforms

import com.google.common.util.concurrent.{ListenableFuture, SettableFuture}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class GuavaFutureHandler extends FutureHandlers.Guava[String]
class JavaFutureHandler extends FutureHandlers.Java[String]

class FutureHandlersTest extends AnyFlatSpec with Matchers {

  def futureHandler[F, I <: F](
    handler: FutureHandlers.Base[F, String],
    create: () => I,
    complete: I => String => Unit,
    fail: I => Throwable => Unit,
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
      complete(f2)("f2 done")
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
      result shouldBe Failure(e)
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
      result shouldBe Failure(e)
      val ee = the[ExecutionException] thrownBy (access(chainedFuture))
      val cause = ee.getCause
      cause.getMessage shouldBe "failure"
      cause.getSuppressed.headOption.map(_.getMessage) shouldBe Some("callback failure")
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
    _.get()
  )

  "Java handler" should behave like futureHandler[
    CompletableFuture[String],
    CompletableFuture[String]
  ](
    new JavaFutureHandler,
    () => new CompletableFuture[String](),
    _.complete,
    _.completeExceptionally,
    _.get()
  )
}
