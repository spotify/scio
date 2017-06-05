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

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * A {@link DoFn} that handles asynchronous requests to an external service.
 */
public abstract class BaseAsyncDoFn<InputT, OutputT, ResourceT, FutureT>
    extends DoFnWithResource<InputT, OutputT, ResourceT> {

  /**
   * Process an element asynchronously.
   */
  public abstract FutureT processElement(InputT input);

  protected abstract void waitForFutures(Iterable<FutureT> futures)
      throws InterruptedException, ExecutionException;
  protected abstract FutureT addCallback(FutureT future,
                                         Function<OutputT, Void> onSuccess,
                                         Function<Throwable, Void> onFailure);

  private final ConcurrentMap<UUID, FutureT> futures = Maps.newConcurrentMap();
  private final ConcurrentLinkedQueue<OutputT> results = Queues.newConcurrentLinkedQueue();
  private final ConcurrentLinkedQueue<Throwable> errors = Queues.newConcurrentLinkedQueue();

  @StartBundle
  public void startBundle(StartBundleContext c) {
    futures.clear();
    results.clear();
    errors.clear();
  }

  @FinishBundle
  public void finishBundle(ProcessContext c) {
    if (!futures.isEmpty()) {
      try {
        waitForFutures(futures.values());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Failed to process futures", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Failed to process futures", e);
      }
    }
    flush(c);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    flush(c);

    final UUID uuid = UUID.randomUUID();
    FutureT future = addCallback(processElement(c.element()), r -> {
      results.add(r);
      futures.remove(uuid);
      return null;
    }, t -> {
      errors.add(t);
      futures.remove(uuid);
      return null;
    });
    // This `put` may happen after `remove` in the callbacks but it's OK since either the result
    // or the error would've already been pushed to the corresponding queues and we are not losing
    // data. `waitForFutures` in `finishBundle` blocks until all pending futures, including ones
    // that may have already completed, and `startBundle` clears everything.
    futures.put(uuid, future);
  }

  private void flush(ProcessContext c) {
    if (!errors.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to process futures");
      Throwable t = errors.poll();
      while (t != null) {
        e.addSuppressed(t);
        t = errors.poll();
      }
      throw e;
    }
    OutputT element = results.poll();
    while (element != null) {
      c.output(element);
      element = results.poll();
    }
  }

}
