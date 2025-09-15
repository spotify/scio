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

package com.spotify.scio.io

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.values.{SCollection, SideOutputCollections}

import scala.annotation.unused

sealed trait TapT[A] extends Serializable {
  type T
  def saveForTest(data: SCollection[A]): Tap[T]
}

object TapT {
  type Aux[A, T0] = TapT[A] { type T = T0 }
}

final class EmptyTapOf[A] private extends TapT[A] {
  override type T = Nothing

  override def saveForTest(data: SCollection[A]): Tap[T] = EmptyTap
}

object EmptyTapOf { def apply[A]: TapT.Aux[A, Nothing] = new EmptyTapOf[A] }

final class TapOf[A] private extends TapT[A] {
  override type T = A

  override def saveForTest(data: SCollection[A]): Tap[T] =
    data.saveAsInMemoryTap.underlying
}

object TapOf { def apply[A]: TapT.Aux[A, A] = new TapOf[A] }

/**
 * Base trait for all Read/Write IO classes. Every IO connector must implement this. This trait has
 * two abstract implicit methods #read, #write that need to be implemented in every subtype. Look at
 * the [[com.spotify.scio.io.TextIO]] subclass for a reference implementation. IO connectors can
 * choose to override #readTest and #writeTest if custom test logic is necessary.
 */
trait ScioIO[T] {

  /** Read parameter type */
  type ReadP

  /** Write parameter type */
  type WriteP

  /**
   * Output tap type.
   *
   * This _must_ be a stable value (a `val`, not a `def`) in every implementation, otherwise the
   * return type of write cannot be inferred.
   */
  val tapT: TapT[T]

  /** Identifier for JobTest IO matching */
  def testId: String = this.toString

  /**
   * Read data according to the read configuration provided in `params` as an SCollection, or return
   * test data if this is in a JobTest
   */
  private[scio] def readWithContext(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.requireNotClosed(if (sc.isTest) readTest(sc, params) else read(sc, params))

  /** Called only in a JobTest. Return test data for this `testId` as an SCollection */
  protected def readTest(sc: ScioContext, @unused params: ReadP): SCollection[T] =
    TestDataManager.getInput(sc.testId.get)(this).toSCollection(sc)

  /** Read data according to the read configuration provided in `params` as an SCollection. */
  protected def read(sc: ScioContext, params: ReadP): SCollection[T]

  /**
   * Write `data` out according to write configuration provided in `params`, or if this is a test
   * write to TestDataManager, returning the Tap type
   */
  private[scio] def writeWithContext(data: SCollection[T], params: WriteP): ClosedTap[tapT.T] = {
    val tap = if (data.context.isTest) writeTest(data, params) else write(data, params)
    ClosedTap(tap)
  }

  /** Write `data` out according to write configuration provided in `params`, returning the Tap type. */
  protected def write(data: SCollection[T], params: WriteP): Tap[tapT.T]

  /** Called only in a JobTest. Write `data` to TestDataManager output and return the Tap type */
  protected def writeTest(data: SCollection[T], @unused params: WriteP): Tap[tapT.T] = {
    TestDataManager.getOutput(data.context.testId.get)(this)(data)
    tapT.saveForTest(data)
  }

  /**
   * Write options also return a `ClosedTap`. Once the job completes you can open the `Tap`. Tap
   * abstracts away the logic of reading the dataset directly as an Iterator[T] or re-opening it in
   * another ScioContext. The Future is complete once the job finishes. This can be used to do light
   * weight pipeline orchestration e.g. WordCountOrchestration.scala.
   */
  def tap(read: ReadP): Tap[tapT.T]
}

object ScioIO {
  type ReadOnly[T, R] =
    ScioIO[T] {
      type ReadP = R
      type WriteP = Nothing
    }

  type Aux[T, R, W] =
    ScioIO[T] {
      type ReadP = R
      type WriteP = W
    }

  def ro[T](io: ScioIO[T]): ScioIO.ReadOnly[T, io.ReadP] =
    new ScioIO[T] {
      override type ReadP = io.ReadP
      override type WriteP = Nothing

      override val tapT: TapT.Aux[T, io.tapT.T] = io.tapT

      override def testId: String = io.testId

      override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
        io.read(sc, params)

      override protected def write(data: SCollection[T], params: WriteP): Tap[io.tapT.T] =
        throw new UnsupportedOperationException("read-only IO. This code should be unreachable")

      override def tap(params: ReadP): Tap[io.tapT.T] = io.tap(params)
    }
}

/** Base trait for [[ScioIO]] without business logic, for stubbing mock data with `JobTest`. */
trait TestIO[T] extends ScioIO[T] {
  override type ReadP = Nothing
  override type WriteP = Nothing

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException(s"$this is for testing purpose only")
  override protected def write(data: SCollection[T], params: WriteP): Tap[tapT.T] =
    throw new UnsupportedOperationException(s"$this is for testing purpose only")
  override def tap(params: ReadP): Tap[tapT.T] =
    throw new UnsupportedOperationException(s"$this is for testing purpose only")
}

trait KeyedIO[T] { self: ScioIO[T] =>
  type KeyT
  def keyBy: T => KeyT
  def keyCoder: Coder[KeyT]
}

trait WriteResultIO[T] { self: ScioIO[T] =>
  override private[scio] def writeWithContext(
    data: SCollection[T],
    params: WriteP
  ): ClosedTap[tapT.T] = {
    if (data.context.isTest) {
      val tap = writeTest(data, params)
      // TODO: add possibility to fill this for testing
      val output = SideOutputCollections.empty(data.context)
      ClosedTap(tap, Some(output))
    } else {
      val (tap, outputs) = writeWithResult(data, params)
      ClosedTap(tap, Some(outputs))
    }
  }

  final override def write(data: SCollection[T], params: self.WriteP): Tap[self.tapT.T] =
    throw new UnsupportedOperationException(
      "WriteResultIO cannot be used with #write. Use #writeWithResult instead"
    )

  protected def writeWithResult(
    data: SCollection[T],
    params: self.WriteP
  ): (Tap[self.tapT.T], SideOutputCollections)
}

/**
 * Special version of [[ScioIO]] for use with [[ScioContext.customInput]] and
 * [[SCollection.saveAsCustomOutput]].
 */
final case class CustomIO[T](id: String) extends TestIO[T] {
  override val tapT: TapT.Aux[T, T] = TapOf[T]
}

/**
 * Special version of [[ScioIO]] for use with [[SCollection.readAll]] and
 * [[SCollection.readAllBytes]].
 */
final case class ReadIO[T](id: String) extends TestIO[T] {
  override val tapT: TapT.Aux[T, T] = TapOf[T]
}
