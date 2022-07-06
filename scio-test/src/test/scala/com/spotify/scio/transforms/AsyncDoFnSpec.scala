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

import java.util.concurrent.CompletableFuture

import com.google.common.util.concurrent.{ListenableFuture, SettableFuture}
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.values.{PCollectionView, TupleTag}
import org.joda.time.Instant
import org.scalacheck.Prop.propBoolean
import org.scalacheck._
import org.scalacheck.commands.Commands

import scala.collection.mutable.{Buffer => MBuffer}
import scala.concurrent.{Future, Promise}

import scala.util.Try

object AsyncDoFnSpec extends Properties("AsyncDoFn") {
  property("GuavaAsyncDoFn") = GuavaAsyncDoFnCommands.property()
  property("JavaAsyncDoFn") = JavaAsyncDoFnCommands.property()
  property("ScalaAsyncDoFn") = ScalaAsyncDoFnCommands.property()
}

object GuavaAsyncDoFnCommands extends AsyncDoFnCommands {
  override def newSut(state: State): Sut = new GuavaAsyncDoFnTester
}

object JavaAsyncDoFnCommands extends AsyncDoFnCommands {
  override def newSut(state: State): Sut = new JavaAsyncDoFnTester
}

object ScalaAsyncDoFnCommands extends AsyncDoFnCommands {
  override def newSut(state: State): Sut = new ScalaAsyncDoFnTester
}

class GuavaAsyncDoFnTester extends AsyncDoFnTester[SettableFuture, ListenableFuture] {
  override def newDoFn: BaseAsyncDoFn[Int, String, Unit, ListenableFuture[String]] =
    new GuavaAsyncDoFn[Int, String, Unit] {
      override def getResourceType: ResourceType = ResourceType.PER_CLASS
      override def processElement(input: Int): ListenableFuture[String] = {
        val p = SettableFuture.create[String]()
        pending.append((input, p))
        p
      }
      override def createResource(): Unit = ()
    }
  override def completePromise(p: SettableFuture[String], result: String): Unit = {
    p.set(result)
    ()
  }
}

class JavaAsyncDoFnTester extends AsyncDoFnTester[CompletableFuture, CompletableFuture] {
  override def newDoFn: BaseAsyncDoFn[Int, String, Unit, CompletableFuture[String]] =
    new JavaAsyncDoFn[Int, String, Unit] {
      override def getResourceType: ResourceType = ResourceType.PER_CLASS
      override def processElement(input: Int): CompletableFuture[String] = {
        val p = new CompletableFuture[String]()
        pending.append((input, p))
        p
      }
      override def createResource(): Unit = ()
    }
  override def completePromise(p: CompletableFuture[String], result: String): Unit = {
    p.complete(result)
    ()
  }
}

class ScalaAsyncDoFnTester extends AsyncDoFnTester[Promise, Future] {
  override def newDoFn: BaseAsyncDoFn[Int, String, Unit, Future[String]] =
    new ScalaAsyncDoFn[Int, String, Unit] {
      override def getResourceType: ResourceType = ResourceType.PER_CLASS
      override def processElement(input: Int): Future[String] = {
        val p = Promise[String]()
        pending.append((input, p))
        p.future
      }
      override def createResource(): Unit = ()
    }
  override def completePromise(p: Promise[String], result: String): Unit =
    p.success(result)
}

trait AsyncDoFnCommands extends Commands {
  case class AsyncDoFnState(total: Int, pending: Int)

  override type State = AsyncDoFnState
  override type Sut = BaseDoFnTester

  override def canCreateNewSut(
    newState: State,
    initSuts: Traversable[State],
    runningSuts: Traversable[Sut]
  ): Boolean = true
  override def destroySut(sut: Sut): Unit = {}
  override def initialPreCondition(state: State): Boolean =
    state == AsyncDoFnState(0, 0)
  override def genInitialState: Gen[State] = Gen.const(AsyncDoFnState(0, 0))

  override def genCommand(state: State): Gen[Command] =
    if (state.pending > 0) {
      Gen.frequency(
        (60, Gen.const(Request)),
        (30, Gen.chooseNum(0, state.pending - 1).map(Complete)),
        (10, Gen.const(NextBundle))
      )
    } else {
      Gen.frequency((90, Gen.const(Request)), (10, Gen.const(NextBundle)))
    }

  case object Request extends UnitCommand {
    override def preCondition(state: State): Boolean = true
    override def postCondition(state: State, success: Boolean): Prop = success
    override def run(sut: Sut): Unit = sut.request()
    override def nextState(state: State): State =
      AsyncDoFnState(state.total + 1, state.pending + 1)
  }

  case class Complete(n: Int) extends UnitCommand {
    override def preCondition(state: State): Boolean = state.pending > 0
    override def postCondition(state: State, success: Boolean): Prop = success
    override def run(sut: Sut): Unit = sut.complete(n)
    override def nextState(state: State): State =
      state.copy(pending = state.pending - 1)
  }

  case object NextBundle extends Command {
    override type Result = Seq[String]
    override def preCondition(state: State): Boolean = true
    override def postCondition(state: State, result: Try[Result]): Prop =
      Prop.all(
        "size" |: state.total == result.get.size,
        "content" |: (0 until state.total)
          .map(_.toString)
          .toSet == result.get.toSet
      )
    override def run(sut: Sut): Result = sut.nextBundle()
    override def nextState(state: State): State = AsyncDoFnState(0, 0)
  }
}

trait BaseDoFnTester {
  def request(): Unit
  def complete(n: Int): Unit
  def nextBundle(): Seq[String]
}

abstract class AsyncDoFnTester[P[_], F[_]] extends BaseDoFnTester {
  private var nextElement = 0
  protected val pending: MBuffer[(Int, P[String])] = MBuffer.empty
  private val outputBuffer = MBuffer.empty[String]

  def newDoFn: BaseAsyncDoFn[Int, String, Unit, F[String]]
  def completePromise(p: P[String], result: String): Unit

  override def toString: String = {
    val p = pending.map(_._1).mkString(",")
    val o = outputBuffer.mkString(",")
    s"$nextElement Pending: [$p] Output: [$o]"
  }

  private val fn = {
    val f = newDoFn
    f.setup()
    f.startBundle(null)
    f
  }

  private val processContext = new fn.ProcessContext {
    override def getPipelineOptions: PipelineOptions = ???
    override def sideInput[T](view: PCollectionView[T]): T = ???
    override def pane(): PaneInfo = ???
    override def output[T](tag: TupleTag[T], output: T): Unit = ???
    override def outputWithTimestamp(output: String, timestamp: Instant): Unit =
      ???
    override def outputWithTimestamp[T](tag: TupleTag[T], output: T, timestamp: Instant): Unit = ???

    // test input
    override def timestamp(): Instant = new Instant(nextElement.toLong)
    override def element(): Int = nextElement
    override def output(output: String): Unit = outputBuffer.append(output)
  }

  private val finishBundleContext = new fn.FinishBundleContext {
    override def getPipelineOptions: PipelineOptions = ???
    override def output[T](
      tag: TupleTag[T],
      output: T,
      timestamp: Instant,
      window: BoundedWindow
    ): Unit = ???
    override def output(output: String, timestamp: Instant, window: BoundedWindow): Unit =
      outputBuffer.append(output)
  }

  // make a new request
  override def request(): Unit = {
    fn.processElement(processContext, null)
    nextElement += 1
  }

  // complete a pending request
  override def complete(n: Int): Unit = {
    val (input, promise) = pending(n)
    completePromise(promise, input.toString)
    pending.remove(n)
    ()
  }

  // finish bundle and start new one
  override def nextBundle(): Seq[String] = {
    pending.foreach { case (input, promise) =>
      completePromise(promise, input.toString)
    }
    fn.finishBundle(finishBundleContext)
    val result = Seq(outputBuffer.toSeq: _*)

    nextElement = 0
    pending.clear()
    outputBuffer.clear()
    fn.startBundle(null)

    result
  }
}
