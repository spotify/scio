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

import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.CheckForNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} that performs asynchronous lookup using the provided client. Lookup requests may
 * be deduplicated.
 *
 * @param <A> input element type.
 * @param <B> client lookup value type.
 * @param <C> client type.
 * @param <F> future type.
 * @param <T> client lookup value type wrapped in a Try.
 */
public abstract class BaseAsyncLookupDoFn<A, B, C, F, T>
    extends DoFnWithResource<A, KV<A, T>, Pair<C, Cache<A, B>>>
    implements FutureHandlers.Base<F, B> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncLookupDoFn.class);

  private final boolean deduplicate;
  private final CacheSupplier<A, B> cacheSupplier;

  // Data structures for handling async requests
  private final int maxPendingRequests;
  private final Semaphore semaphore;
  private final ConcurrentMap<UUID, F> futures = new ConcurrentHashMap<>();
  private final ConcurrentMap<A, F> inFlightRequests = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<Pair<UUID, Result>> results = new ConcurrentLinkedQueue<>();
  private long inputCount;
  private long outputCount;

  /** Creates the client. */
  protected abstract C newClient();

  /** Perform asynchronous lookup. */
  public abstract F asyncLookup(C client, A input);

  /** Wrap output in a successful Try. */
  public abstract T success(B output);

  /** Wrap output in a failed Try. */
  public abstract T failure(Throwable throwable);

  /** Create a {@link BaseAsyncLookupDoFn} instance. */
  public BaseAsyncLookupDoFn() {
    this(1000);
  }

  /**
   * Create a {@link BaseAsyncLookupDoFn} instance. Simultaneous requests for the same input may be
   * de-duplicated.
   *
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   */
  public BaseAsyncLookupDoFn(int maxPendingRequests) {
    this(maxPendingRequests, true, new NoOpCacheSupplier<>());
  }

  /**
   * Create a {@link BaseAsyncLookupDoFn} instance. Simultaneous requests for the same input may be
   * de-duplicated.
   *
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public BaseAsyncLookupDoFn(int maxPendingRequests, CacheSupplier<A, B> cacheSupplier) {
    this(maxPendingRequests, true, cacheSupplier);
  }

  /**
   * Create a {@link BaseAsyncLookupDoFn} instance.
   *
   * @param maxPendingRequests maximum number of pending requests on every cloned DoFn. This
   *     prevents runner from timing out and retrying bundles.
   * @param deduplicate if an attempt should be made to de-duplicate simultaneous requests for the
   *     same input
   * @param cacheSupplier supplier for lookup cache.
   */
  public BaseAsyncLookupDoFn(
      int maxPendingRequests, boolean deduplicate, CacheSupplier<A, B> cacheSupplier) {
    this.maxPendingRequests = maxPendingRequests;
    this.deduplicate = deduplicate;
    this.cacheSupplier = cacheSupplier;
    this.semaphore = new Semaphore(maxPendingRequests);
  }

  @Override
  public Pair<C, Cache<A, B>> createResource() {
    return Pair.of(newClient(), cacheSupplier.get());
  }

  public C getResourceClient() {
    return getResource().getLeft();
  }

  public Cache<A, B> getResourceCache() {
    return getResource().getRight();
  }

  @StartBundle
  public void startBundle(StartBundleContext context) {
    futures.clear();
    results.clear();
    inFlightRequests.clear();
    inputCount = 0;
    outputCount = 0;
    semaphore.drainPermits();
    semaphore.release(maxPendingRequests);
  }

  @SuppressWarnings("unchecked")
  @ProcessElement
  public void processElement(
      @Element A input,
      @Timestamp Instant timestamp,
      OutputReceiver<KV<A, T>> out,
      BoundedWindow window) {
    inputCount++;
    flush(r -> out.output(KV.of(r.input, r.output)));
    final C client = getResourceClient();
    final Cache<A, B> cache = getResourceCache();

    try {
      final UUID key = UUID.randomUUID();
      final B cached = cache.getIfPresent(input);
      final F inFlight = inFlightRequests.get(input);
      if (cached != null) {
        // found in cache
        out.output(KV.of(input, success(cached)));
        outputCount++;
      } else if (inFlight != null) {
        // pending request for the same element
        futures.put(key, handleOutput(inFlight, input, key, timestamp, window));
      } else {
        // semaphore release is not performed on exception.
        // let beam retry the bundle. startBundle will reset the semaphore to the
        // maxPendingRequests permits.
        semaphore.acquire();
        final F future = asyncLookup(client, input);
        // handle cache in fire & forget way
        handleCache(future, input, cache);
        // make sure semaphore are released when waiting for futures in finishBundle
        final F unlockedFuture = handleSemaphore(future);
        futures.put(key, handleOutput(unlockedFuture, input, key, timestamp, window));
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to acquire semaphore", e);
      throw new RuntimeException("Failed to acquire semaphore", e);
    } catch (Exception e) {
      LOG.error("Failed to process element", e);
      throw e;
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    if (!futures.isEmpty()) {
      try {
        // Block until all pending futures are complete
        waitForFutures(futures.values());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Failed to process futures", e);
        throw new RuntimeException("Failed to process futures", e);
      } catch (ExecutionException e) {
        LOG.error("Failed to process futures", e);
        throw new RuntimeException("Failed to process futures", e);
      }
    }
    flush(r -> context.output(KV.of(r.input, r.output), r.timestamp, r.window));

    // Make sure all requests are processed
    Preconditions.checkState(
        inputCount == outputCount,
        "Expected inputCount == outputCount, but %s != %s",
        inputCount,
        outputCount);
  }

  private F handleOutput(F future, A input, UUID key, Instant timestamp, BoundedWindow window) {
    return addCallback(
        future,
        output -> {
          final Result result = new Result(input, success(output), timestamp, window);
          results.add(Pair.of(key, result));
          return null;
        },
        throwable -> {
          final Result result = new Result(input, failure(throwable), timestamp, window);
          results.add(Pair.of(key, result));
          return null;
        });
  }

  private F handleSemaphore(F future) {
    return addCallback(
        future,
        ouput -> {
          semaphore.release();
          return null;
        },
        throwable -> {
          semaphore.release();
          return null;
        });
  }

  private F handleCache(F future, A input, Cache<A, B> cache) {
    final boolean shouldRemove =
        deduplicate && (inFlightRequests.putIfAbsent(input, future) == null);
    return addCallback(
        future,
        output -> {
          cache.put(input, output);
          if (shouldRemove) inFlightRequests.remove(input);
          return null;
        },
        throwable -> {
          if (shouldRemove) inFlightRequests.remove(input);
          return null;
        });
  }

  // Flush pending errors and results
  private void flush(Consumer<Result> outputFn) {
    Pair<UUID, Result> r = results.poll();
    while (r != null) {
      final UUID key = r.getKey();
      final Result result = r.getValue();
      outputFn.accept(result);
      outputCount++;
      futures.remove(key);
      r = results.poll();
    }
  }

  private class Result {
    private A input;
    private T output;
    private Instant timestamp;
    private BoundedWindow window;

    Result(A input, T output, Instant timestamp, BoundedWindow window) {
      this.input = input;
      this.output = output;
      this.timestamp = timestamp;
      this.window = window;
    }
  }

  /**
   * Encapsulate lookup that may be success or failure.
   *
   * @param <A> lookup value type.
   */
  public static class Try<A> implements Serializable {
    private final boolean isSuccess;
    private final A value;
    private final Throwable exception;

    public Try(A value) {
      isSuccess = true;
      this.value = value;
      this.exception = null;
    }

    public Try(Throwable exception) {
      Preconditions.checkNotNull(exception, "exception must not be null");
      isSuccess = false;
      this.value = null;
      this.exception = exception;
    }

    public A get() {
      return value;
    }

    public Throwable getException() {
      return exception;
    }

    public boolean isSuccess() {
      return isSuccess;
    }

    public boolean isFailure() {
      return !isSuccess;
    }

    @Override
    public int hashCode() {
      if (isSuccess) {
        return value == null ? 0 : value.hashCode();
      } else {
        return exception.hashCode();
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Try)) {
        return false;
      }
      Try<?> that = (Try<?>) obj;
      return (this.isSuccess == that.isSuccess)
          && Objects.equals(this.get(), that.get())
          && Objects.equals(this.getException(), that.getException());
    }
  }

  /**
   * {@link Cache} supplier for {@link BaseAsyncLookupDoFn}.
   *
   * @param <K> key type.
   * @param <V> value type
   */
  @FunctionalInterface
  public interface CacheSupplier<K, V> extends Supplier<Cache<K, V>>, Serializable {}

  public static class NoOpCacheSupplier<K, V> implements CacheSupplier<K, V> {

    @Override
    public Cache<K, V> get() {
      return new AbstractCache<K, V>() {
        @Override
        public void put(K key, V value) {}

        @CheckForNull
        @Override
        public V getIfPresent(Object key) {
          return null;
        }
      };
    }
  }
}
