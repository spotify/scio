/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.transforms;

import static java.util.Objects.requireNonNull;

import com.google.common.cache.Cache;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} that performs asynchronous lookup using the provided client. Lookup requests may
 * be deduplicated.
 *
 * @param <Input> input element type.
 * @param <BatchRequest> batched input element type
 * @param <BatchResponse> batched output element type
 * @param <Output> client lookup value type.
 * @param <ClientType> client type.
 * @param <FutureType> future type.
 * @param <TryWrapper> client lookup value type wrapped in a Try.
 */
public abstract class BatchedBaseAsyncLookupDoFn<
        Input, BatchRequest, BatchResponse, Output, ClientType, FutureType, TryWrapper>
    extends DoFnWithResource<Input, KV<Input, TryWrapper>, Pair<ClientType, Cache<String, Output>>>
    implements FutureHandlers.Base<FutureType, BatchResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchedBaseAsyncLookupDoFn.class);

  // Data structures for handling async requests
  private final int maxPendingRequests;
  private final int batchSize;
  private final Semaphore semaphore;
  private final ConcurrentMap<UUID, FutureType> futures = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Triple<Input, Instant, BoundedWindow>> inputsMap =
      new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<Result> results = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Pair<UUID, Input>> inputElements =
      new ConcurrentLinkedQueue<>();
  private long elementsRequestedCount;
  private long resultCount;
  private final SerializableFunction<List<Input>, BatchRequest> batchRequestFn;
  private final SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn;
  private final SerializableFunction<Input, String> idExtractorFn;
  private final CacheSupplier<String, Output> cacheSupplier;

  public BatchedBaseAsyncLookupDoFn(
      int batchSize,
      int maxPendingRequests,
      CacheSupplier<String, Output> cacheSupplier,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn) {
    this.cacheSupplier = cacheSupplier;
    this.batchRequestFn = batchRequestFn;
    this.batchResponseFn = batchResponseFn;
    this.idExtractorFn = idExtractorFn;
    this.maxPendingRequests = maxPendingRequests;
    this.batchSize = batchSize;
    this.semaphore = new Semaphore(maxPendingRequests);
  }

  protected abstract ClientType newClient();

  public abstract FutureType asyncLookup(ClientType client, BatchRequest input);

  public abstract TryWrapper success(Output output);

  public abstract TryWrapper failure(Throwable throwable);

  @Override
  public Pair<ClientType, Cache<String, Output>> createResource() {
    return Pair.of(newClient(), cacheSupplier.get());
  }

  public ClientType getResourceClient() {
    return getResource().getLeft();
  }

  public Cache<String, Output> getResourceCache() {
    return getResource().getRight();
  }

  @StartBundle
  public void startBundle(StartBundleContext context) {
    futures.clear();
    results.clear();
    inputsMap.clear();
    elementsRequestedCount = 0;
    resultCount = 0;
    semaphore.drainPermits();
    semaphore.release(maxPendingRequests);
  }

  @ProcessElement
  public void processElement(
      @Element Input input,
      @Timestamp Instant timestamp,
      OutputReceiver<KV<Input, TryWrapper>> out,
      BoundedWindow window) {
    flush(r -> out.output(KV.of(r.input, r.output)));
    final Cache<String, Output> cache = getResourceCache();

    try {
      final String inputId = this.idExtractorFn.apply(input);
      requireNonNull(inputId, "idExtractorFn returned null");

      final Output cached = cache.getIfPresent(inputId);

      if (cached != null) {
        // found in cache
        out.output(KV.of(input, success(cached)));
      } else {
        final UUID uuid = UUID.randomUUID();
        inputElements.offer(Pair.of(uuid, input));
        inputsMap.put(inputId, Triple.of(input, timestamp, window));
      }

      if (inputElements.size() >= batchSize) {
        final List<Pair<UUID, Input>> elementsToBatch =
            inputElements.stream()
                .limit(batchSize)
                .peek(inputElements::remove)
                .collect(Collectors.toList());
        createRequests(elementsToBatch);
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

    // send remaining
    final List<Pair<UUID, Input>> remainingElements =
        inputElements.stream().peek(inputElements::remove).collect(Collectors.toList());

    try {
      /** @todo handle exception properly * */
      createRequests(remainingElements);
      if (!futures.isEmpty()) {
        // Block until all pending futures are complete
        waitForFutures(futures.values());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Failed to process futures", e);
      throw new RuntimeException("Failed to process futures", e);
    } catch (ExecutionException e) {
      LOG.error("Failed to process futures", e);
      throw new RuntimeException("Failed to process futures", e);
    }
    flush(r -> context.output(KV.of(r.input, r.output), r.timestamp, r.window));

    // Make sure all requests are processed
    Preconditions.checkState(
        elementsRequestedCount == resultCount,
        "Expected elementsRequestedCount == resultCount, but %s != %s",
        elementsRequestedCount,
        resultCount);
  }

  private void createRequests(List<Pair<UUID, Input>> elementsToBatch) throws InterruptedException {
    final ClientType client = getResourceClient();
    final Cache<String, Output> cache = getResourceCache();
    final UUID uuid = UUID.randomUUID();

    final List<Input> inputsOnly =
        elementsToBatch.stream().map(Pair::getRight).collect(Collectors.toList());

    final BatchRequest batch = batchRequestFn.apply(inputsOnly);

    // semaphore release is not performed on exception.
    // let beam retry the bundle. startBundle will reset the semaphore to the
    // maxPendingRequests permits.
    semaphore.acquire();
    final FutureType future = asyncLookup(client, batch);

    // handle cache in fire & forget way
    handleCache(future, cache);
    // make sure semaphore are released when waiting for futures in finishBundle
    final FutureType unlockedFuture = handleSemaphore(future);

    futures.put(uuid, handleOutput(unlockedFuture, inputsOnly, uuid));
    elementsRequestedCount += elementsToBatch.size();
  }

  private FutureType handleOutput(FutureType future, List<Input> batchInput, UUID key) {
    return addCallback(
        future,
        response -> {
          batchResponseFn
              .apply(response)
              .forEach(
                  pair -> {
                    final Triple<Input, Instant, BoundedWindow> originalInput =
                        inputsMap.get(pair.getLeft());
                    final Input input = originalInput.getLeft();
                    final String inputID = this.idExtractorFn.apply(input);
                    final Output output = pair.getRight();
                    results.add(
                        new Result(
                            input,
                            success(output),
                            key,
                            originalInput.getMiddle(),
                            originalInput.getRight()));
                    this.inputsMap.remove(inputID);
                  });
          return null;
        },
        throwable -> {
          batchInput.forEach(
              input -> {
                final String inputID = this.idExtractorFn.apply(input);
                final Triple<Input, Instant, BoundedWindow> originalInput = inputsMap.get(inputID);
                results.add(
                    new Result(
                        input,
                        failure(throwable),
                        key,
                        originalInput.getMiddle(),
                        originalInput.getRight()));
                this.inputsMap.remove(inputID);
              });
          return null;
        });
  }

  private FutureType handleSemaphore(FutureType future) {
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

  private FutureType handleCache(FutureType future, Cache<String, Output> cache) {
    return addCallback(
        future,
        response -> {
          batchResponseFn
              .apply(response)
              .forEach(
                  pair -> {
                    final String inputID = pair.getLeft();
                    final Output output = pair.getRight();

                    cache.put(inputID, output);
                  });
          return null;
        },
        throwable -> {
          return null;
        });
  }

  // Flush pending elements errors and results
  private void flush(Consumer<Result> outputFn) {
    Result r = results.poll();
    while (r != null) {
      outputFn.accept(r);
      resultCount++;
      futures.remove(r.futureUuid);
      r = results.poll();
    }
  }

  private class Result {

    private Input input;
    private TryWrapper output;
    private UUID futureUuid;
    private Instant timestamp;
    private BoundedWindow window;

    Result(
        Input input, TryWrapper output, UUID futureUuid, Instant timestamp, BoundedWindow window) {
      this.input = input;
      this.output = output;
      this.futureUuid = futureUuid;
      this.timestamp = timestamp;
      this.window = window;
    }
  }
}
