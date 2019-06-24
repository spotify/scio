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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

/**
 * A {@link DoFn} that performs asynchronous lookup using the provided client.
 * @param <A> input element type.
 * @param <B> client lookup value type.
 * @param <C> client type.
 * @param <F> future type.
 * @param <T> client lookup value type wrapped in a Try.
 */
public abstract class BaseAsyncLookupDoFn<A, B, C, F, T>
    extends DoFn<A, KV<A, T>>
    implements FutureHandlers.Base<F, B> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncLookupDoFn.class);

  // DoFn is deserialized once per CPU core. We assign a unique UUID to each DoFn instance upon
  // creation, so that all cloned instances share the same ID. This ensures all cores share the
  // same Client and Cache.
  private static final ConcurrentMap<UUID, Object> client = Maps.newConcurrentMap();
  private static final ConcurrentMap<UUID, Cache> cache = Maps.newConcurrentMap();
  private final UUID instanceId;

  private final CacheSupplier<A, B, ?> cacheSupplier;

  // Data structures for handling async requests
  private final Semaphore semaphore;
  private final ConcurrentMap<UUID, F> futures = Maps.newConcurrentMap();
  private final ConcurrentLinkedQueue<Result> results = Queues.newConcurrentLinkedQueue();
  private long requestCount;
  private long resultCount;

  /**
   * Perform asynchronous lookup.
   */
  public abstract F asyncLookup(C client, A input);

  /** Wrap output in a successful Try. */
  public abstract T success(B output);

  /** Wrap output in a failed Try. */
  public abstract T failure(Throwable throwable);

  /**
   * Create a {@link BaseAsyncLookupDoFn} instance.
   */
  public BaseAsyncLookupDoFn() {
    this(1000);
  }

  /**
   * Create a {@link BaseAsyncLookupDoFn} instance.
   * @param maxPendingRequests maximum number of pending requests to prevent runner from timing out
   *                           and retrying bundles.
   */
  public BaseAsyncLookupDoFn(int maxPendingRequests) {
    this(maxPendingRequests, new NoOpCacheSupplier<>());
  }

  /**
   * Create a {@link BaseAsyncLookupDoFn} instance.
   * @param maxPendingRequests maximum number of pending requests to prevent runner from timing out
   *                           and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public <K> BaseAsyncLookupDoFn(int maxPendingRequests, CacheSupplier<A, B, K> cacheSupplier) {
    this.instanceId = UUID.randomUUID();
    this.cacheSupplier = cacheSupplier;
    this.semaphore = new Semaphore(maxPendingRequests);
  }

  protected abstract C newClient();

  @Setup
  public void setup() {
    client.computeIfAbsent(instanceId, instanceId -> newClient());
    cache.computeIfAbsent(instanceId, instanceId -> cacheSupplier.createCache());
  }

  @StartBundle
  public void startBundle() {
    futures.clear();
    results.clear();
    requestCount = 0;
    resultCount = 0;
  }

  @SuppressWarnings("unchecked")
  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    flush(r -> c.output(KV.of(r.input, r.output)));

    final A input = c.element();
    B cached = cacheSupplier.get(instanceId, input);
    if (cached != null) {
      c.output(KV.of(input, success(cached)));
      return;
    }

    final UUID uuid = UUID.randomUUID();
    F future;
    try {
      semaphore.acquire();
      try {
        //noinspection unchecked
        future = asyncLookup((C) client.get(instanceId), input);
      } catch (Exception e) {
        semaphore.release();
        LOG.error("Failed to process element", e);
        throw e;
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to acquire semaphore", e);
      throw new RuntimeException("Failed to acquire semaphore", e);
    }
    requestCount++;

    F f = addCallback(future, output -> {
      semaphore.release();
      try {
        cacheSupplier.put(instanceId, input, output);
        results.add(new Result(input, success(output), c.timestamp(), window));
        futures.remove(uuid);
      } catch (Exception e) {
        LOG.error("Failed to cache result", e);
        throw e;
      }
      return null;
    }, throwable -> {
      semaphore.release();
      results.add(new Result(input, failure(throwable), c.timestamp(), window));
      futures.remove(uuid);
      return null;
    });

    // This `put` may happen after `remove` in the callbacks but it's OK since either the result
    // or the error would've already been pushed to the corresponding queues and we are not losing
    // data. `waitForFutures` in `finishBundle` blocks until all pending futures, including ones
    // that may have already completed, and `startBundle` clears everything.
    futures.put(uuid, f);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
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
      }
    }
    flush(r -> c.output(KV.of(r.input, r.output), r.timestamp, r.window));

    // Make sure all requests are processed
    Preconditions.checkState(requestCount == resultCount);
  }

  // Flush pending errors and results
  private void flush(Consumer<Result> outputFn) {
    Result r = results.poll();
    while (r != null) {
      outputFn.accept(r);
      resultCount++;
      r = results.poll();
    }
  }

  private class Result {
    private A input;
    private T output;
    private Instant timestamp;
    private BoundedWindow window;

    Result(A input, T output, Instant timestamp, BoundedWindow window) {
      this.input  = input;
      this.output = output;
      this.timestamp = timestamp;
      this.window = window;
    }
  }

  /**
   * Encapsulate lookup that may be success or failure.
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

    public boolean isFailure() { return !isSuccess; }

    @Override
    public int hashCode() {
      return isSuccess ? value.hashCode() : exception.hashCode();
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
      return (this.isSuccess == that.isSuccess) && Objects.equals(this.get(), that.get()) &&
          Objects.equals(this.getException(), that.getException());
    }
  }

  /**
   * {@link Cache} supplier for {@link BaseAsyncLookupDoFn}.
   * @param <A> input element type.
   * @param <B> lookup value type.
   * @param <K> key type.
   */
  public static abstract class CacheSupplier<A, B, K> implements Serializable {
    /**
     * Create a new {@link Cache} instance. This is called once per
     * {@link BaseAsyncLookupDoFn} instance.
     */
    public abstract Cache<K, B> createCache();

    /**
     * Get cache key for the input element.
     */
    public abstract K getKey(A input);

    @SuppressWarnings("unchecked")
    public B get(UUID instanceId, A item) {
      Cache<K, B> c = cache.get(instanceId);
      return c == null ? null : c.getIfPresent(getKey(item));
    }

    @SuppressWarnings("unchecked")
    public void put(UUID instanceId, A item, B value) {
      Cache<K, B> c = cache.get(instanceId);
      if (c != null) {
        c.put(getKey(item), value);
      }
    }
  }

  public static class NoOpCacheSupplier<A, B> extends CacheSupplier<A, B, String> {
    @Override
    public Cache<String, B> createCache() {
      return null;
    }

    @Override
    public String getKey(A input) {
      return null;
    }
  }
}
