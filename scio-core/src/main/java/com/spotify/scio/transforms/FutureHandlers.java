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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/** Utility to abstract away Guava, Java 8 and Scala future handling. */
public class FutureHandlers {

  /**
   * Base interface for future handling.
   * @param <F> future type.
   * @param <V> value type.
   */
  public interface Base<F, V> {
    void waitForFutures(Iterable<F> futures)
            throws InterruptedException, ExecutionException;
    F addCallback(F future, Function<V, Void> onSuccess, Function<Throwable, Void> onFailure);
  }

  /** A {@link Base} implementation for Guava {@link ListenableFuture}. */
  public interface Guava<V> extends Base<ListenableFuture<V>, V> {
    @Override
    default void waitForFutures(Iterable<ListenableFuture<V>> futures)
            throws InterruptedException, ExecutionException {
      Futures.allAsList(futures).get();
    }

    @Override
    default ListenableFuture<V> addCallback(ListenableFuture<V> future, Function<V, Void> onSuccess, Function<Throwable, Void> onFailure) {
      Futures.addCallback(future, new FutureCallback<V>() {
        @Override
        public void onSuccess(@Nullable V result) {}

        @Override
        public void onFailure(Throwable t) {
          onFailure.apply(t);
        }
      }, MoreExecutors.directExecutor());

      return Futures.transform(future, new com.google.common.base.Function<V, V>() {
        @Nullable
        @Override
        public V apply(@Nullable V input) {
          onSuccess.apply(input);
          return input;
        }
      }, MoreExecutors.directExecutor());
    }
  }

  /** A {@link Base} implementation for Java 8 {@link CompletableFuture}. */
  public interface Java<V> extends Base<CompletableFuture<V>, V> {
    @Override
    default void waitForFutures(Iterable<CompletableFuture<V>> futures)
            throws InterruptedException, ExecutionException {
      CompletableFuture.allOf(Iterables.toArray(futures, CompletableFuture.class)).get();
    }

    @Override
    default CompletableFuture<V> addCallback(CompletableFuture<V> future,
                                             Function<V, Void> onSuccess,
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

}
