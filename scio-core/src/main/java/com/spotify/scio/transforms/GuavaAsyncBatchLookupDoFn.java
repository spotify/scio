/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.transforms;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.Try;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @param <Input> input element type.
 * @param <BatchRequest> batched input element type
 * @param <BatchResponse> batched output element type
 * @param <Output> client lookup value type.
 * @param <ClientType> client type.
 */
public abstract class GuavaAsyncBatchLookupDoFn<
        Input, BatchRequest, BatchResponse, Output, ClientType>
    extends BaseAsyncBatchLookupDoFn<
        Input,
        BatchRequest,
        BatchResponse,
        Output,
        ClientType,
        ListenableFuture<BatchResponse>,
        Try<Output>>
    implements FutureHandlers.Guava<BatchResponse> {

  public GuavaAsyncBatchLookupDoFn(
      int batchSize,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn,
      int maxPendingRequests) {
    super(batchSize, batchRequestFn, batchResponseFn, idExtractorFn, maxPendingRequests);
  }

  public GuavaAsyncBatchLookupDoFn(
      int batchSize,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn,
      int maxPendingRequests,
      CacheSupplier<String, Output> cacheSupplier) {
    super(
        batchSize,
        batchRequestFn,
        batchResponseFn,
        idExtractorFn,
        maxPendingRequests,
        cacheSupplier);
  }

  @Override
  public Try<Output> success(Output output) {
    return new Try<>(output);
  }

  @Override
  public Try<Output> failure(Throwable throwable) {
    return new Try<>(throwable);
  }
}
