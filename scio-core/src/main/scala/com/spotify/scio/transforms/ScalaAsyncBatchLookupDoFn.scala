/*
 * Copyright 2024 Spotify AB.
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
import com.spotify.scio.util.Functions
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.commons.lang3.tuple.Pair

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

/**
 * A [[org.apache.beam.sdk.transforms.DoFn DoFn]] that performs asynchronous lookup using the
 * provided client for Scala [[Future]].
 *
 * @tparam Input
 *   input element type.
 * @tparam BatchRequest
 *   batched input element type
 * @tparam BatchResponse
 *   batched output element type
 * @tparam Output
 *   client lookup value type.
 * @tparam Client
 *   client type.
 */
abstract class ScalaAsyncBatchLookupDoFn[Input, BatchRequest, BatchResponse, Output, Client](
  batchSize: Int,
  batchRequestFn: Iterable[Input] => BatchRequest,
  batchResponseFn: BatchResponse => Iterable[(String, Output)],
  idExtractorFn: Input => String,
  maxPendingRequests: Int,
  cacheSupplier: CacheSupplier[String, Output] = new NoOpCacheSupplier[String, Output]()
) extends BaseAsyncBatchLookupDoFn[
      Input,
      BatchRequest,
      BatchResponse,
      Output,
      Client,
      Future[BatchResponse],
      Try[Output]
    ](
      batchSize,
      ScalaAsyncBatchLookupDoFn.javaBatchRequestFn(batchRequestFn),
      ScalaAsyncBatchLookupDoFn.javaBatchResponseFn(batchResponseFn),
      Functions.serializableFn(idExtractorFn),
      maxPendingRequests,
      cacheSupplier
    )
    with ScalaFutureHandlers[BatchResponse] {

  override def success(output: Output): Try[Output] = Success(output)
  override def failure(throwable: Throwable): Try[Output] = Failure(throwable)
}

object ScalaAsyncBatchLookupDoFn {
  private def javaBatchRequestFn[Input, BatchRequest](
    batchRequestFn: Iterable[Input] => BatchRequest
  ): SerializableFunction[java.util.List[Input], BatchRequest] = {
    val fn = batchRequestFn.compose[java.util.List[Input]](_.asScala)
    Functions.serializableFn(fn)
  }

  private def javaBatchResponseFn[BatchResponse, Output](
    batchResponseFn: BatchResponse => Iterable[(String, Output)]
  ): SerializableFunction[BatchResponse, java.util.List[Pair[String, Output]]] = {
    val fn = batchResponseFn.andThen(_.map { case (id, out) => Pair.of(id, out) }.toList.asJava)
    Functions.serializableFn(fn)
  }
}
