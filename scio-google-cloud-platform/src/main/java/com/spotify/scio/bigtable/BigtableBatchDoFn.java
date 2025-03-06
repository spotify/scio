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

package com.spotify.scio.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn;
import com.spotify.scio.transforms.GuavaAsyncBatchLookupDoFn;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A {@link DoFn} which batches elements and performs asynchronous lookup for them using Google
 * Cloud Bigtable.
 *
 * @param <Input> input element type.
 * @param <BatchRequest> batched input element type
 * @param <BatchResponse> batched response from BigTable type
 * @param <Result> Bigtable lookup value type.
 */
public abstract class BigtableBatchDoFn<Input, BatchRequest, BatchResponse, Result>
    extends GuavaAsyncBatchLookupDoFn<
        Input, BatchRequest, BatchResponse, Result, BigtableDataClient> {

  private final Supplier<BigtableDataSettings> settingsSupplier;

  /** Perform asynchronous Bigtable lookup. */
  public abstract ListenableFuture<BatchResponse> asyncLookup(
      BigtableDataClient client, BatchRequest batchRequest);

  /**
   * Create a {@link BigtableBatchDoFn} instance.
   *
   * @param settingsSupplier Bigtable data settings supplier.
   */
  public BigtableBatchDoFn(
      Supplier<BigtableDataSettings> settingsSupplier,
      int batchSize,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Result>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn) {
    this(settingsSupplier, batchSize, batchRequestFn, batchResponseFn, idExtractorFn, 1000);
  }

  /**
   * Create a {@link BigtableBatchDoFn} instance.
   *
   * @param settingsSupplier Bigtable data settings supplier.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   */
  public BigtableBatchDoFn(
      Supplier<BigtableDataSettings> settingsSupplier,
      int batchSize,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Result>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn,
      int maxPendingRequests) {
    this(
        settingsSupplier,
        batchSize,
        batchRequestFn,
        batchResponseFn,
        idExtractorFn,
        maxPendingRequests,
        new BaseAsyncLookupDoFn.NoOpCacheSupplier<>());
  }

  /**
   * Create a {@link BigtableBatchDoFn} instance.
   *
   * @param settingsSupplier Bigtable data settings supplier.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public BigtableBatchDoFn(
      Supplier<BigtableDataSettings> settingsSupplier,
      int batchSize,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Result>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn,
      int maxPendingRequests,
      BaseAsyncLookupDoFn.CacheSupplier<String, Result> cacheSupplier) {
    super(
        batchSize,
        batchRequestFn,
        batchResponseFn,
        idExtractorFn,
        maxPendingRequests,
        cacheSupplier);
    this.settingsSupplier = settingsSupplier;
  }

  @Override
  public ResourceType getResourceType() {
    // BigtableSession is backed by a gRPC thread safe client
    return ResourceType.PER_INSTANCE;
  }

  protected BigtableDataClient newClient() {
    try {
      return BigtableDataClient.create(settingsSupplier.get());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
