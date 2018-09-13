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
import com.spotify.scio.values.SCollection

import scala.concurrent.Future

/**
 * Base trait for all Read/Write IO classes. Every IO connector must implement this.
 * This trait has two abstract implicit methods #read, #write that need to be implemented
 * in every subtype. Look at the [[com.spotify.scio.io.TextIO]] subclass for a reference
 * implementation.
 */
trait ScioIO[T] {
  // abstract types for read/write params.
  type ReadP
  type WriteP

  // identifier for JobTest IO matching
  def testId: String = this.toString

  def read(sc: ScioContext, params: ReadP): SCollection[T]

  def write(data: SCollection[T], params: WriteP): Future[Tap[T]]

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

final case class CustomIO[T](id: String) extends TestIO[T]
