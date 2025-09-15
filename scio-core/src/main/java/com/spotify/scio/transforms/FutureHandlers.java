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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/** Utility to abstract away Guava, Java 8 and Scala future handling. */
public class FutureHandlers {

  /**
   * Base interface for future handling.
   *
   * @param <F> future type.
   * @param <V> value type.
   */
  public interface Base<F, V> {

    default Duration getTimeout() {
      return Duration.ofMinutes(10);
    }

    void waitForFutures(Iterable<F> futures)
        throws InterruptedException, ExecutionException, TimeoutException;

    F addCallback(F future, Function<V, Void> onSuccess, Function<Throwable, Void> onFailure);
  }

  /** A {@link Base} implementation for Guava {@link ListenableFuture}. */
  public interface Guava<V> extends Base<ListenableFuture<V>, V> {

    /**
     * Executor used for callbacks. Default is {@link ForkJoinPool#commonPool()}. Consider
     * overriding this method if callbacks are blocking.
     *
     * @return Executor for callbacks.
     */
    default Executor getCallbackExecutor() {
      return ForkJoinPool.commonPool();
    }

    @Override
    default void waitForFutures(Iterable<ListenableFuture<V>> futures)
        throws InterruptedException, ExecutionException, TimeoutException {
      // use Future#successfulAsList instead of Futures#allAsList which only works if all
      // futures succeed
      ListenableFuture<?> f = Futures.successfulAsList(futures);
      Duration timeout = getTimeout();
      if (timeout != null) {
        f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } else {
        f.get();
      }
    }

    @Override
    default ListenableFuture<V> addCallback(
        ListenableFuture<V> future,
        Function<V, Void> onSuccess,
        Function<Throwable, Void> onFailure) {
      // Futures#transform doesn't allow onFailure callback while Futures#addCallback doesn't
      // guarantee that callbacks are called before ListenableFuture#get() unblocks
      SettableFuture<V> f = SettableFuture.create();
      // if executor rejects the callback, we have to fail the future
      Executor rejectPropagationExecutor =
          command -> {
            try {
              getCallbackExecutor().execute(command);
            } catch (RejectedExecutionException e) {
              f.setException(e);
            }
          };
      Futures.addCallback(
          future,
          new FutureCallback<V>() {
            @Override
            public void onSuccess(@Nullable V result) {
              try {
                onSuccess.apply(result);
                f.set(result);
              } catch (Throwable e) {
                f.setException(e);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              Throwable callbackException = null;
              try {
                onFailure.apply(t);
              } catch (Throwable e) {
                // do not fail executing thread if callback fails
                // record exception and propagate as suppressed
                callbackException = e;
              } finally {
                if (callbackException != null) {
                  t.addSuppressed(callbackException);
                }
                f.setException(t);
              }
            }
          },
          rejectPropagationExecutor);

      return f;
    }
  }

  /** A {@link Base} implementation for Java 8 {@link CompletableFuture}. */
  public interface Java<V> extends Base<CompletableFuture<V>, V> {
    @Override
    default void waitForFutures(Iterable<CompletableFuture<V>> futures)
        throws InterruptedException, ExecutionException, TimeoutException {
      CompletableFuture[] array =
          StreamSupport.stream(futures.spliterator(), false).toArray(CompletableFuture[]::new);
      CompletableFuture<?> f = CompletableFuture.allOf(array).exceptionally(t -> null);
      Duration timeout = getTimeout();
      if (timeout != null) {
        f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } else {
        f.get();
      }
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
