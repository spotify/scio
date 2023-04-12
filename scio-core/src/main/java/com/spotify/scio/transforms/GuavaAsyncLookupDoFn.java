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

package com.spotify.scio.transforms;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A {@link DoFn} that performs asynchronous lookup using the provided client for Guava {@link
 * ListenableFuture}.
 *
 * @param <A> input element type.
 * @param <B> client lookup value type.
 * @param <C> client type.
 */
public abstract class GuavaAsyncLookupDoFn<A, B, C>
    extends BaseAsyncLookupDoFn<A, B, C, ListenableFuture<B>, BaseAsyncLookupDoFn.Try<B>>
    implements FutureHandlers.Guava<B> {

  /** Create a {@link GuavaAsyncLookupDoFn} instance. */
  public GuavaAsyncLookupDoFn() {
    super();
  }

  /**
   * Create a {@link GuavaAsyncLookupDoFn} instance.
   *
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   */
  public GuavaAsyncLookupDoFn(int maxPendingRequests) {
    super(maxPendingRequests);
  }

  /**
   * Create a {@link GuavaAsyncLookupDoFn} instance.
   *
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public GuavaAsyncLookupDoFn(
      int maxPendingRequests, BaseAsyncLookupDoFn.CacheSupplier<A, B> cacheSupplier) {
    super(maxPendingRequests, cacheSupplier);
  }

  /**
   * Create a {@link GuavaAsyncLookupDoFn} instance.
   *
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param deduplicate if an attempt should be made to de-duplicate simultaneous requests for the
   *     same input
   * @param cacheSupplier supplier for lookup cache.
   */
  public GuavaAsyncLookupDoFn(
      int maxPendingRequests,
      boolean deduplicate,
      BaseAsyncLookupDoFn.CacheSupplier<A, B> cacheSupplier) {
    super(maxPendingRequests, deduplicate, cacheSupplier);
  }

  @Override
  public BaseAsyncLookupDoFn.Try<B> success(B output) {
    return new Try<>(output);
  }

  @Override
  public BaseAsyncLookupDoFn.Try<B> failure(Throwable throwable) {
    return new Try<>(throwable);
  }
}
