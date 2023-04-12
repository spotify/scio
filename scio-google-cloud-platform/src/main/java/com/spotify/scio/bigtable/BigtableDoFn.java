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

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn;
import com.spotify.scio.transforms.GuavaAsyncLookupDoFn;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A {@link DoFn} that performs asynchronous lookup using Google Cloud Bigtable.
 *
 * @param <A> input element type.
 * @param <B> Bigtable lookup value type.
 */
public abstract class BigtableDoFn<A, B> extends GuavaAsyncLookupDoFn<A, B, BigtableSession> {

  private final BigtableOptions options;

  /** Perform asynchronous Bigtable lookup. */
  public abstract ListenableFuture<B> asyncLookup(BigtableSession session, A input);

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param options Bigtable options.
   */
  public BigtableDoFn(BigtableOptions options) {
    this(options, 1000);
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param options Bigtable options.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   */
  public BigtableDoFn(BigtableOptions options, int maxPendingRequests) {
    this(options, maxPendingRequests, new BaseAsyncLookupDoFn.NoOpCacheSupplier<>());
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param options Bigtable options.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public BigtableDoFn(
      BigtableOptions options,
      int maxPendingRequests,
      BaseAsyncLookupDoFn.CacheSupplier<A, B> cacheSupplier) {
    super(maxPendingRequests, cacheSupplier);
    this.options = options;
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   *
   * @param options Bigtable options.
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param deduplicate if an attempt should be made to de-duplicate simultaneous requests for the
   *     same input
   * @param cacheSupplier supplier for lookup cache.
   */
  public BigtableDoFn(
      BigtableOptions options,
      int maxPendingRequests,
      boolean deduplicate,
      BaseAsyncLookupDoFn.CacheSupplier<A, B> cacheSupplier) {
    super(maxPendingRequests, deduplicate, cacheSupplier);
    this.options = options;
  }

  @Override
  public ResourceType getResourceType() {
    // BigtableSession is backed by a gRPC thread safe client
    return ResourceType.PER_INSTANCE;
  }

  protected BigtableSession newClient() {
    try {
      return new BigtableSession(options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
