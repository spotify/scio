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

import com.google.common.util.concurrent.*;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/** Utility to abstract away Guava, Java 8 and Scala future handling. */
public class FutureHandlers {

  /**
   * Base interface for future handling.
   *
   * @param <F> future type.
   * @param <V> value type.
   */
  public interface Base<F, V> {
    void waitForFutures(Iterable<F> futures) throws InterruptedException, ExecutionException;

    F addCallback(F future, Function<V, Void> onSuccess, Function<Throwable, Void> onFailure);
  }

  /** A {@link Base} implementation for Guava {@link ListenableFuture}. */
  public interface Guava<V> extends Base<ListenableFuture<V>, V> {
    @Override
    default void waitForFutures(Iterable<ListenableFuture<V>> futures)
        throws InterruptedException, ExecutionException {
      // Futures#allAsList only works if all futures succeed
      Futures.whenAllComplete(futures).run(() -> {}, MoreExecutors.directExecutor()).get();
    }

    @Override
    default ListenableFuture<V> addCallback(
        ListenableFuture<V> future,
        Function<V, Void> onSuccess,
        Function<Throwable, Void> onFailure) {
      // Futures#transform doesn't allow onFailure callback while Futures#addCallback doesn't
      // guarantee that callbacks are called before ListenableFuture#get() unblocks
      SettableFuture<V> f = SettableFuture.create();
      Futures.addCallback(
          future,
          new FutureCallback<V>() {
            @Override
            public void onSuccess(@Nullable V result) {
              try {
                onSuccess.apply(result);
              } catch (RuntimeException | Error e) {
                f.setException(e);
                return;
              }
              f.set(result);
            }

            @Override
            public void onFailure(Throwable t) {
              try {
                onFailure.apply(t);
              } finally {
                f.setException(t);
              }
            }
          },
          MoreExecutors.directExecutor());

      return f;
    }
  }

  /** A {@link Base} implementation for Java 8 {@link CompletableFuture}. */
  public interface Java<V> extends Base<CompletableFuture<V>, V> {
    @Override
    default void waitForFutures(Iterable<CompletableFuture<V>> futures)
        throws InterruptedException, ExecutionException {
      CompletableFuture[] array =
          StreamSupport.stream(futures.spliterator(), false).toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(array).get();
    }

    @Override
    default CompletableFuture<V> addCallback(
        CompletableFuture<V> future,
        Function<V, Void> onSuccess,
        Function<Throwable, Void> onFailure) {
      return future.whenComplete(
          (r, t) -> {
            if (r != null) {
              onSuccess.apply(r);
            } else {
              onFailure.apply(t);
            }
          });
    }
  }
}
