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
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.CheckForNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} that performs asynchronous lookup using the provided client. Lookup requests may
 * be deduplicated.
 *
 * @param <Input> input element type.
 * @param <Output> client lookup value type.
 * @param <Client> client type.
 * @param <Future> future type.
 * @param <TryWrapper> client lookup value type wrapped in a Try.
 */
public abstract class BaseAsyncLookupDoFn<Input, Output, Client, Future, TryWrapper>
    extends DoFnWithResource<Input, KV<Input, TryWrapper>, Pair<Client, Cache<Input, Output>>>
    implements FutureHandlers.Base<Future, Output> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncLookupDoFn.class);

  private final boolean deduplicate;
  private final CacheSupplier<Input, Output> cacheSupplier;

  // Data structures for handling async requests
  private final int maxPendingRequests;
  private final Semaphore semaphore;
  private final ConcurrentMap<UUID, Future> futures = new ConcurrentHashMap<>();
  private final ConcurrentMap<Input, Future> inFlightRequests = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<Pair<UUID, ValueInSingleWindow<KV<Input, TryWrapper>>>>
      results = new ConcurrentLinkedQueue<>();
  private long inputCount;
  private long outputCount;

  /** Creates the client. */
  protected abstract Client newClient();

  /** Perform asynchronous lookup. */
  public abstract Future asyncLookup(Client client, Input input);

  /** Wrap output in a successful Try. */
  public abstract TryWrapper success(Output output);

  /** Wrap output in a failed Try. */
  public abstract TryWrapper failure(Throwable throwable);

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
  public BaseAsyncLookupDoFn(int maxPendingRequests, CacheSupplier<Input, Output> cacheSupplier) {
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
      int maxPendingRequests, boolean deduplicate, CacheSupplier<Input, Output> cacheSupplier) {
    this.maxPendingRequests = maxPendingRequests;
    this.deduplicate = deduplicate;
    this.cacheSupplier = cacheSupplier;
    this.semaphore = new Semaphore(maxPendingRequests);
  }

  @Override
  public Pair<Client, Cache<Input, Output>> createResource() {
    return Pair.of(newClient(), cacheSupplier.get());
  }

  @Override
  public void closeResource(Pair<Client, Cache<Input, Output>> resource) throws Exception {
    final Client client = resource.getLeft();
    if (client instanceof AutoCloseable) {
      ((AutoCloseable) client).close();
    }
  }

  public Client getResourceClient() {
    return getResource().getLeft();
  }

  public Cache<Input, Output> getResourceCache() {
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

  // kept for binary compatibility. Must not be used
  // TODO: remove in 0.15.0
  @Deprecated
  public void processElement(
      Input input,
      Instant timestamp,
      OutputReceiver<KV<Input, TryWrapper>> out,
      BoundedWindow window) {
    processElement(input, timestamp, window, null, out);
  }

  @SuppressWarnings("unchecked")
  @ProcessElement
  public void processElement(
      @Element Input input,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo pane,
      OutputReceiver<KV<Input, TryWrapper>> out) {
    inputCount++;
    flush(
        r -> {
          final KV<Input, TryWrapper> io = r.getValue();
          final Instant ts = r.getTimestamp();
          final Collection<BoundedWindow> ws = Collections.singleton(r.getWindow());
          final PaneInfo p = r.getPane();
          out.outputWindowedValue(io, ts, ws, p);
        });
    final Client client = getResourceClient();
    final Cache<Input, Output> cache = getResourceCache();

    try {
      final UUID key = UUID.randomUUID();
      final Output cached = cache.getIfPresent(input);
      final Future inFlight = inFlightRequests.get(input);
      if (cached != null) {
        // found in cache
        out.output(KV.of(input, success(cached)));
        outputCount++;
      } else if (inFlight != null) {
        // pending request for the same element
        futures.put(key, handleOutput(inFlight, input, key, timestamp, window, pane));
      } else {
        // semaphore release is not performed on exception.
        // let beam retry the bundle. startBundle will reset the semaphore to the
        // maxPendingRequests permits.
        semaphore.acquire();
        final Future future = asyncLookup(client, input);
        // handle cache in fire & forget way
        handleCache(future, input, cache);
        // make sure semaphore are released when waiting for futures in finishBundle
        final Future unlockedFuture = handleSemaphore(future);
        futures.put(key, handleOutput(unlockedFuture, input, key, timestamp, window, pane));
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
      } catch (ExecutionException | TimeoutException e) {
        LOG.error("Failed to process futures", e);
        throw new RuntimeException("Failed to process futures", e);
      }
    }
    flush(r -> context.output(r.getValue(), r.getTimestamp(), r.getWindow()));

    // Make sure all requests are processed
    Preconditions.checkState(
        inputCount == outputCount,
        "Expected inputCount == outputCount, but %s != %s",
        inputCount,
        outputCount);
  }

  private Future handleOutput(
      Future future,
      Input input,
      UUID key,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo pane) {
    return addCallback(
        future,
        output -> {
          final ValueInSingleWindow<KV<Input, TryWrapper>> result =
              ValueInSingleWindow.of(KV.of(input, success(output)), timestamp, window, pane);
          results.add(Pair.of(key, result));
          return null;
        },
        throwable -> {
          final ValueInSingleWindow<KV<Input, TryWrapper>> result =
              ValueInSingleWindow.of(KV.of(input, failure(throwable)), timestamp, window, pane);
          results.add(Pair.of(key, result));
          return null;
        });
  }

  private Future handleSemaphore(Future future) {
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

  private Future handleCache(Future future, Input input, Cache<Input, Output> cache) {
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
  private void flush(Consumer<ValueInSingleWindow<KV<Input, TryWrapper>>> outputFn) {
    Pair<UUID, ValueInSingleWindow<KV<Input, TryWrapper>>> r = results.poll();
    while (r != null) {
      final UUID key = r.getKey();
      final ValueInSingleWindow<KV<Input, TryWrapper>> result = r.getValue();
      outputFn.accept(result);
      outputCount++;
      futures.remove(key);
      r = results.poll();
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
