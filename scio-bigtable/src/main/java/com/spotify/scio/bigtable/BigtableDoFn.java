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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

/**
 * A {@link DoFn} that performs asynchronous lookup using Google Cloud Bigtable.
 * @param <A> input element type.
 * @param <B> Bigtable lookup value type.
 */
public abstract class BigtableDoFn<A, B> extends DoFn<A, KV<A, B>> {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableDoFn.class);

  // DoFn is deserialized once per CPU core. We assign a unique UUID to each DoFn instance upon
  // creation, so that all cloned instances share the same ID. This ensures all cores share the
  // same BigtableSession and Cache.
  private static final ConcurrentMap<UUID, BigtableSession> session = Maps.newConcurrentMap();
  private static final ConcurrentMap<UUID, Cache> cache = Maps.newConcurrentMap();
  private final UUID instanceId;

  private final BigtableOptions options;
  private final CacheSupplier<A, B, ?> cacheSupplier;

  // Data structures for handling async requests
  private final Semaphore semaphore;
  private final ConcurrentMap<UUID, ListenableFuture<B>> futures = Maps.newConcurrentMap();
  private final ConcurrentLinkedQueue<Result> results = Queues.newConcurrentLinkedQueue();
  private final ConcurrentLinkedQueue<Throwable> errors = Queues.newConcurrentLinkedQueue();
  private long requestCount;
  private long resultCount;

  /**
   * Perform asynchronous Bigtable lookup.
   */
  public abstract ListenableFuture<B> asyncLookup(BigtableSession session, A input);

  /**
   * Create a {@link BigtableDoFn} instance.
   * @param options Bigtable options.
   */
  public BigtableDoFn(BigtableOptions options) {
    this(options, 1000);
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   * @param options Bigtable options.
   * @param maxPendingRequests maximum number of pending requests to prevent runner from timing out
   *                           and retrying bundles.
   */
  public BigtableDoFn(BigtableOptions options,
                      int maxPendingRequests) {
    this(options, maxPendingRequests, new NoOpCacheSupplier<>());
  }

  /**
   * Create a {@link BigtableDoFn} instance.
   * @param options Bigtable options.
   * @param maxPendingRequests maximum number of pending requests to prevent runner from timing out
   *                           and retrying bundles.
   * @param cacheSupplier supplier for lookup cache.
   */
  public <K> BigtableDoFn(BigtableOptions options,
                          int maxPendingRequests,
                          CacheSupplier<A, B, K> cacheSupplier) {
    this.instanceId = UUID.randomUUID();
    this.options = options;
    this.cacheSupplier = cacheSupplier;
    this.semaphore = new Semaphore(maxPendingRequests);
  }

  @Setup
  public void setup() {
    LOG.info("Setup for {}", this);
    session.computeIfAbsent(instanceId, instanceId -> {
      LOG.info("Creating BigtableSession with BigtableOptions {}", options);
      try {
        // options can be null for testing
        return options == null ? null : new BigtableSession(options);
      } catch (IOException e) {
        e.printStackTrace();
        LOG.error("Failed to create BigtableSession", e);
        throw new RuntimeException("Failed to create BigtableSession", e);
      }
    });
    cache.computeIfAbsent(instanceId, instanceId -> cacheSupplier.createCache());
  }

  @StartBundle
  public void startBundle() {
    LOG.info("Start bundle for {}", this);
    futures.clear();
    results.clear();
    errors.clear();
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
      c.output(KV.of(input, cached));
      return;
    }

    final UUID uuid = UUID.randomUUID();
    ListenableFuture<B> future;
    try {
      semaphore.acquire();
      try {
        future = asyncLookup(session.get(instanceId), input);
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

    // Handle failure
    Futures.addCallback(future, new FutureCallback<B>() {
      @Override
      public void onSuccess(@Nullable B result) {
        semaphore.release();
      }

      @Override
      public void onFailure(Throwable t) {
        semaphore.release();
        errors.add(t);
        futures.remove(uuid);
      }
    });

    // Handle success
    ListenableFuture<B> f = Futures.transform(future, new Function<B, B>() {
      @Nullable
      @Override
      public B apply(@Nullable B output) {
        cacheSupplier.put(instanceId, input, output);
        results.add(new Result(input, output, c.timestamp(), window));
        futures.remove(uuid);
        return output;
      }
    });

    // This `put` may happen after `remove` in the callbacks but it's OK since either the result
    // or the error would've already been pushed to the corresponding queues and we are not losing
    // data. `waitForFutures` in `finishBundle` blocks until all pending futures, including ones
    // that may have already completed, and `startBundle` clears everything.
    futures.put(uuid, f);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    LOG.info("Finish bundle for {}", this);
    if (!futures.isEmpty()) {
      try {
        // Block until all pending futures are complete
        Futures.allAsList(futures.values()).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Failed to process futures", e);
        throw new RuntimeException("Failed to process futures", e);
      } catch (ExecutionException e) {
        LOG.error("Failed to process futures", e);
        throw new RuntimeException("Failed to process futures", e);
      }
    }
    flush(r -> c.output(KV.of(r.input, r.output), r.timestamp, r.window));

    // Make sure all requests are processed
    Preconditions.checkState(requestCount == resultCount);
  }

  // Flush pending errors and results
  private void flush(Consumer<Result> outputFn) {
    if (!errors.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to process futures");
      Throwable t = errors.poll();
      while (t != null) {
        e.addSuppressed(t);
        t = errors.poll();
      }
      LOG.error("Failed to process futures", e);
      throw e;
    }
    Result r = results.poll();
    while (r != null) {
      outputFn.accept(r);
      resultCount++;
      r = results.poll();
    }
  }

  private class Result {
    private A input;
    private B output;
    private Instant timestamp;
    private BoundedWindow window;

    Result(A input, B output, Instant timestamp, BoundedWindow window) {
      this.input  = input;
      this.output = output;
      this.timestamp = timestamp;
      this.window = window;
    }
  }

  /**
   * Supplier for {@link BigtableDoFn} with caching logic.
   * @param <A> input element type.
   * @param <B> Bigtable lookup value type.
   * @param <K> key type.
   */
  public static abstract class CacheSupplier<A, B, K> implements Serializable {
    /**
     * Create a new {@link Cache} instance. This is called once per {@link BigtableDoFn} instance.
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

  private static class NoOpCacheSupplier<A, B> extends CacheSupplier<A, B, String> {
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
