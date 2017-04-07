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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.transforms.DoFn;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * A {@link DoFn} that handles asynchronous requests to an external service that returns Guava
 * {@link ListenableFuture}s.
 */
public abstract class GuavaAsyncDoFn<InputT, OutputT, ResourceT>
    extends BaseAsyncDoFn<InputT, OutputT, ResourceT, ListenableFuture<OutputT>> {
  @Override
  protected void waitForFutures(Iterable<ListenableFuture<OutputT>> futures)
      throws InterruptedException, ExecutionException {
    Futures.allAsList(futures).get();
  }

  @Override
  protected ListenableFuture<OutputT> addCallback(ListenableFuture<OutputT> future,
                                                  Function<OutputT, Void> onSuccess,
                                                  Function<Throwable, Void> onFailure) {
    Futures.addCallback(future, new FutureCallback<OutputT>() {
      @Override
      public void onSuccess(@Nullable OutputT result) {
        onSuccess.apply(result);
      }

      @Override
      public void onFailure(Throwable t) {
        onFailure.apply(t);
      }
    });
    return future;
  }
}
