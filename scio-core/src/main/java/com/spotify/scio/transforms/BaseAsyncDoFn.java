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

package com.spotify.scio.transforms;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} that handles asynchronous requests to an external service. */
public abstract class BaseAsyncDoFn<InputT, OutputT, ResourceT, FutureT>
    extends DoFnWithResource<InputT, OutputT, ResourceT>
    implements FutureHandlers.Base<FutureT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncDoFn.class);

  /** Process an element asynchronously. */
  public abstract FutureT processElement(InputT input);

  private final ConcurrentMap<UUID, FutureT> futures = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<Result> results = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

  @StartBundle
  public void startBundle(StartBundleContext context) {
    futures.clear();
    results.clear();
    errors.clear();
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    if (!futures.isEmpty()) {
      try {
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
    flush(context);
  }

  @ProcessElement
  public void processElement(
      @Element InputT element,
      @Timestamp Instant timestamp,
      OutputReceiver<OutputT> out,
      BoundedWindow window) {
    flush(out);

    try {
      final UUID uuid = UUID.randomUUID();
      final FutureT future = processElement(element);
      futures.put(uuid, handleOutput(future, uuid, timestamp, window));
    } catch (Exception e) {
      LOG.error("Failed to process element", e);
      throw e;
    }
  }

  private FutureT handleOutput(FutureT future, UUID key, Instant timestamp, BoundedWindow window) {
    return addCallback(
        future,
        output -> {
          results.add(new Result(output, key, timestamp, window));
          return null;
        },
        throwable -> {
          errors.add(throwable);
          return null;
        });
  }

  private void flush(OutputReceiver<OutputT> outputReceiver) {
    if (!errors.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to process futures");
      Throwable t = errors.poll();
      while (t != null) {
        e.addSuppressed(t);
        t = errors.poll();
      }
      throw e;
    }
    Result r = results.poll();
    while (r != null) {
      outputReceiver.output(r.output);
      futures.remove(r.futureUuid);
      r = results.poll();
    }
  }

  private void flush(FinishBundleContext c) {
    if (!errors.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to process futures");
      Throwable t = errors.poll();
      while (t != null) {
        e.addSuppressed(t);
        t = errors.poll();
      }
      throw e;
    }
    Result r = results.poll();
    while (r != null) {
      c.output(r.output, r.timestamp, r.window);
      futures.remove(r.futureUuid);
      r = results.poll();
    }
  }

  private class Result {
    private OutputT output;
    private UUID futureUuid;
    private Instant timestamp;
    private BoundedWindow window;

    Result(OutputT output, UUID futureUuid, Instant timestamp, BoundedWindow window) {
      this.output = output;
      this.timestamp = timestamp;
      this.futureUuid = futureUuid;
      this.window = window;
    }
  }
}
