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

import org.apache.beam.sdk.transforms.DoFn;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link DoFn} that performs asynchronous lookup using the provided client for Java 8
 * {@link CompletableFuture}.
 * @param <A> input element type.
 * @param <B> client lookup value type.
 * @param <C> client type.
 */
public abstract class JavaAsyncLookupDoFn<A, B, C>
    extends BaseAsyncLookupDoFn<A, B, C, CompletableFuture<B>, BaseAsyncLookupDoFn.Try<B>>
    implements FutureHandlers.Java<B> {

  /**
   * Create a {@link GuavaAsyncLookupDoFn} instance.
   */
  public JavaAsyncLookupDoFn() {
    super();
  }

  /**
   * Create a {@link JavaAsyncLookupDoFn} instance.
   * @param maxPendingRequests maximum number of pending requests to prevent runner from timing out
   *                           and retrying bundles.
   */
  public JavaAsyncLookupDoFn(int maxPendingRequests) {
    super(maxPendingRequests);
  }

  /**
   * Create a {@link JavaAsyncLookupDoFn} instance.
   * @param maxPendingRequests maximum number of pending requests to prevent runner from timing out
   *                           and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public <K> JavaAsyncLookupDoFn(int maxPendingRequests,
                                 BaseAsyncLookupDoFn.CacheSupplier<A, B, K> cacheSupplier) {
    super(maxPendingRequests, cacheSupplier);
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
