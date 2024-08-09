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

package com.spotify.scio.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn;
import com.spotify.scio.transforms.GuavaAsyncLookupDoFn;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A {@link DoFn} that performs asynchronous lookup using Google Cloud Bigtable.
 *
 * @param <A> input element type.
 * @param <B> Bigtable lookup value type.
 */
public abstract class BigtableDoFn<A, B> extends GuavaAsyncLookupDoFn<A, B, BigtableDataClient> {

  private final Supplier<BigtableDataSettings> settingsSupplier;

  /** Perform asynchronous Bigtable lookup. */
  public abstract ListenableFuture<B> asyncLookup(BigtableDataClient client, A input);

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param settingsSupplier Bigtable data settings supplier.
   */
  public BigtableDoFn(Supplier<BigtableDataSettings> settingsSupplier) {
    this(settingsSupplier, 1000);
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param settingsSupplier Bigtable data settings supplier.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   */
  public BigtableDoFn(Supplier<BigtableDataSettings> settingsSupplier, int maxPendingRequests) {
    this(settingsSupplier, maxPendingRequests, new BaseAsyncLookupDoFn.NoOpCacheSupplier<>());
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param settingsSupplier Bigtable data settings supplier.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public BigtableDoFn(
      Supplier<BigtableDataSettings> settingsSupplier,
      int maxPendingRequests,
      BaseAsyncLookupDoFn.CacheSupplier<A, B> cacheSupplier) {
    super(maxPendingRequests, cacheSupplier);
    this.settingsSupplier = settingsSupplier;
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param settingsSupplier Bigtable data settings supplier.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param deduplicate if an attempt should be made to de-duplicate simultaneous requests for the
   *     same input
   * @param cacheSupplier supplier for lookup cache.
   */
  public BigtableDoFn(
      Supplier<BigtableDataSettings> settingsSupplier,
      int maxPendingRequests,
      boolean deduplicate,
      BaseAsyncLookupDoFn.CacheSupplier<A, B> cacheSupplier) {
    super(maxPendingRequests, deduplicate, cacheSupplier);
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
