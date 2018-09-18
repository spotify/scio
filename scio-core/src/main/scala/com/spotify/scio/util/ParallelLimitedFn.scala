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

package com.spotify.scio.util

import java.util.concurrent.Semaphore

import com.spotify.scio.transforms.DoFnWithResource
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Utility class to limit the number of parallel doFns
 * @param maxDoFns Max number of doFns
 */
private[scio] abstract class ParallelLimitedFn[T, U](maxDoFns: Int)
    extends DoFnWithResource[T, U, Semaphore]
    with NamedFn {

  def getResourceType: ResourceType = ResourceType.PER_CLASS

  def createResource: Semaphore = new Semaphore(maxDoFns, true)

  def parallelProcessElement(x: DoFn[T, U]#ProcessContext): Unit

  @ProcessElement def processElement(x: DoFn[T, U]#ProcessContext): Unit = {
    val semaphore = getResource
    try {
      semaphore.acquire()
      parallelProcessElement(x)
    } finally {
      semaphore.release()
    }
  }
}
