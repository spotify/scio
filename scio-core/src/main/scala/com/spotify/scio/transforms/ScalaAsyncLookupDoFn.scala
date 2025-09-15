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

import com.spotify.scio.transforms.BaseAsyncLookupDoFn.{CacheSupplier, NoOpCacheSupplier}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * A [[org.apache.beam.sdk.transforms.DoFn DoFn]] that performs asynchronous lookup using the
 * provided client for Scala [[Future]].
 * @tparam Input
 *   input element type.
 * @tparam Output
 *   client lookup value type.
 * @tparam Client
 *   client type.
 */
abstract class ScalaAsyncLookupDoFn[Input, Output, Client](
  maxPendingRequests: Int,
  deduplicate: Boolean,
  cacheSupplier: CacheSupplier[Input, Output]
) extends BaseAsyncLookupDoFn[Input, Output, Client, Future[Output], Try[Output]](
      maxPendingRequests,
      deduplicate,
      cacheSupplier
    )
    with ScalaFutureHandlers[Output] {
  def this() =
    this(1000, true, new NoOpCacheSupplier[Input, Output])

  /**
   * @param maxPendingRequests
   *   maximum number of pending requests on every cloned DoFn. This prevents runner from timing out
   *   and retrying bundles.
   */
  def this(maxPendingRequests: Int) =
    this(maxPendingRequests, true, new NoOpCacheSupplier[Input, Output])

  /**
   * @param maxPendingRequests
   *   maximum number of pending requests on every cloned DoFn. This prevents runner from timing out
   *   and retrying bundles.
   * @param cacheSupplier
   *   supplier for lookup cache.
   */
  def this(maxPendingRequests: Int, cacheSupplier: CacheSupplier[Input, Output]) =
    this(maxPendingRequests, true, cacheSupplier)

  override def success(output: Output): Try[Output] = Success(output)
  override def failure(throwable: Throwable): Try[Output] = Failure(throwable)
}
