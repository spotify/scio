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

package com.spotify

/**
 * Main package for public APIs. Import all.
 *
 * {{{
 * import com.spotify.scio._
 * }}}
 */
package object scio {
  import scala.concurrent.duration.Duration
  import scala.concurrent.Future
  import scala.concurrent.Await
  import com.spotify.scio.io.Tap
  @deprecated(
    "waitForResult is deprecated since Scio 0.8 does not rely on Future anymore." +
      " see https://spotify.github.io/scio/migrations/v0.8.0.html#scala-concurrent-future-removed-from-scioios for more information",
    since = "0.8.0"
  )
  implicit class WaitableFutureTap[T](self: Future[Tap[T]]) {
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] =
      Await.result(self, atMost)
  }

  @deprecated(
    "waitForResult is deprecated since Scio 0.8 does not rely on Future anymore." +
      " see https://spotify.github.io/scio/migrations/v0.8.0.html#scala-concurrent-future-removed-from-scioios for more information",
    since = "0.8.0"
  )
  implicit class WaitableClosedTap[T](self: com.spotify.scio.io.ClosedTap[T]) {
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] =
      self.underlying
  }

  /**
   * Wait for nested [[com.spotify.scio.io.Tap Tap]] to be available, flatten result and get Tap
   * reference from `Future`.
   */
  @deprecated(
    "waitForResult is deprecated since Scio 0.8 does not rely on Future anymore." +
      " see https://spotify.github.io/scio/migrations/v0.8.0.html#scala-concurrent-future-removed-from-scioios for more information",
    since = "0.8.0"
  )
  implicit class WaitableNestedFutureTap[T](self: Future[Future[Tap[T]]]) {
    import scala.concurrent.ExecutionContext.Implicits.global
    def waitForResult(atMost: Duration = Duration.Inf): Tap[T] =
      Await.result(self.flatMap(identity), atMost)
  }
}
