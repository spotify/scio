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

package com.spotify.scio

import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.testing.TestStream

import scala.reflect.ClassTag

package object testing {

  /** Create a `TestStream.Builder` instance. */
  def testStreamOf[T: ClassTag]: TestStream.Builder[T] =
    TestStream.create(ScioUtil.getScalaCoder[T])

  /** Enhanced version of [[ScioContext]] with streaming methods. */
  implicit class TestStreamScioContext(val self: ScioContext) extends AnyVal {
    /** Distribute a local `TestStream` to form an SCollection. */
    def testStream[T: ClassTag](ts: TestStream[T]): SCollection[T] =
      self.wrap(self.pipeline.apply(ts.toString, ts))
  }

  type TextIO = nio.TextIO
  val TextIO = nio.TextIO

  type DatastoreIO = nio.DatastoreIO
  val DatastoreIO = nio.DatastoreIO

}
