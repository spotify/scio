/*
 * Copyright 2016 Spotify AB.
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
import com.spotify.scio.values.SCollection

import scala.concurrent.Future

/**
 * Base trait for all Read/Write IO classes. Every IO connector must implement this.
 * This trait has two abstract implicit methods #read, #write that need to be implemented
 * in every subtype. Look at the [[com.spotify.scio.io.TextIO]] subclass for a reference
 * implementation. IO connectors can choose to override #readTest and #writeTest if custom
 * test logic is necessary.
 */
trait ScioIO[T] {
  // abstract types for read/write params.
  type ReadP
  type WriteP

  // identifier for JobTest IO matching
  def testId: String = this.toString

  protected[scio] def readWithContext(sc: ScioContext, params: ReadP)(
    implicit coder: Coder[T]): SCollection[T] =
    sc.requireNotClosed {
      if (sc.isTest) {
        readTest(sc, params)
      } else {
        read(sc, params)
      }
    }

  protected[scio] def writeWithContext(data: SCollection[T], params: WriteP)(
    implicit coder: Coder[T]): Future[Tap[T]] =
    if (data.context.isTest) {
      writeTest(data, params)
    } else {
      write(data, params)
    }

  protected def read(sc: ScioContext, params: ReadP): SCollection[T]

  protected def write(data: SCollection[T], params: WriteP): Future[Tap[T]]

  protected def readTest(sc: ScioContext, params: ReadP)(
    implicit coder: Coder[T]): SCollection[T] = {
    sc.parallelize(
      TestDataManager.getInput(sc.testId.get)(this).asInstanceOf[Seq[T]]
    )
  }

  protected def writeTest(data: SCollection[T], params: WriteP)(
    implicit coder: Coder[T]): Future[Tap[T]] = {
    TestDataManager.getOutput(data.context.testId.get)(this)(data)
    data.saveAsInMemoryTap
  }

  def tap(params: ReadP): Tap[T]
}

object ScioIO {
  // scalastyle:off structural.type
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

      override def testId: String = io.testId

      override def read(sc: ScioContext, params: ReadP): SCollection[T] =
        io.read(sc, params)

      override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] =
        throw new IllegalStateException("read-only IO. This code should be unreachable")

      override def tap(params: ReadP): Tap[T] =
        io.tap(params)
    }
  // scalastyle:on structural.type
}

/** Base trait for [[ScioIO]] without business logic, for stubbing mock data with `JobTest`. */
trait TestIO[T] extends ScioIO[T] {
  override type ReadP = Nothing
  override type WriteP = Nothing

  override def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new IllegalStateException(s"$this is for testing purpose only")
  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] =
    throw new IllegalStateException(s"$this is for testing purpose only")
  override def tap(params: ReadP): Tap[T] =
    throw new IllegalStateException(s"$this is for testing purpose only")
}

/**
 * Special version of [[ScioIO]] for use with [[ScioContext.customInput]] and
 * [[SCollection.saveAsCustomOutput]].
 */
final case class CustomIO[T](id: String) extends TestIO[T]

/**
 * Special version of [[ScioIO]] for use with [[SCollection.readAll]] and
 * [[SCollection.readAllBytes]].
 */
final case class ReadIO[T](id: String) extends TestIO[T]
