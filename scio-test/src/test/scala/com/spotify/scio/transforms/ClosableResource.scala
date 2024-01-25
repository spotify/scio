/*
 * Copyright 2024 Spotify AB
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

import java.util.concurrent.atomic.AtomicInteger

object ClosableResourceCounters {
  val clientsOpen = new AtomicInteger(0)
  val clientsOpened = new AtomicInteger(0)

  def resetCounters(): Unit = {
    clientsOpen.set(0)
    clientsOpened.set(0)
  }

  def allResourcesClosed: Boolean = clientsOpen.get() == 0
}

trait CloseableResource extends AutoCloseable {

  ClosableResourceCounters.clientsOpened.incrementAndGet()
  ClosableResourceCounters.clientsOpen.incrementAndGet()

  def ensureOpen(): Unit = {
    if (ClosableResourceCounters.allResourcesClosed) {
      throw new Exception("Called when it was closed")
    }
  }

  override def close(): Unit =
    ClosableResourceCounters.clientsOpen.decrementAndGet()
}
