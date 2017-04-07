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

package com.spotify.scio.extra.transforms;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * A {@link DoFn} that handles asynchronous requests to an external service that returns Java 8
 * {@link CompletableFuture}s.
 */
public abstract class JavaAsyncDoFn<InputT, OutputT, ResourceT>
    extends BaseAsyncDoFn<InputT, OutputT, ResourceT, CompletableFuture<OutputT>> {
  @SuppressWarnings("unchecked")
  @Override
  protected void waitForFutures(Iterable<CompletableFuture<OutputT>> futures)
      throws InterruptedException, ExecutionException {
    CompletableFuture.allOf(Iterables.toArray(futures, CompletableFuture.class)).get();
  }

  @Override
  protected CompletableFuture<OutputT> addCallback(CompletableFuture<OutputT> future,
                                                   Function<OutputT, Void> onSuccess,
                                                   Function<Throwable, Void> onFailure) {
    return future.whenComplete((r, t) -> {
      if (r != null) {
        onSuccess.apply(r);
      } else {
        onFailure.apply(t);
      }
    });
  }
}
