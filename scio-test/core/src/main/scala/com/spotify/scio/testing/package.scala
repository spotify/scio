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

package com.spotify.scio

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.testing.TestStream

package object testing {

  /** Create a `TestStream.Builder` instance. */
  def testStreamOf[T: Coder]: TestStream.Builder[T] =
    TestStream.create(CoderMaterializer.beamWithDefault(Coder[T]))

  /** Enhanced version of [[ScioContext]] with streaming methods. */
  implicit class TestStreamScioContext(private val self: ScioContext) extends AnyVal {

    /** Distribute a local `TestStream` to form an SCollection. */
    def testStream[T](ts: TestStream[T]): SCollection[T] =
      self.wrap(self.pipeline.apply(ts.toString, ts))
  }
}
