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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} that handles asynchronous requests to an external service. */
public abstract class BaseAsyncDoFn<Input, Output, Resource, Future>
    extends DoFnWithResource<Input, Output, Resource>
    implements FutureHandlers.Base<Future, Output> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncDoFn.class);

  /** Process an element asynchronously. */
  public abstract Future processElement(Input input);

  private final ConcurrentMap<UUID, Future> futures = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<Pair<UUID, ValueInSingleWindow<Output>>> results =
      new ConcurrentLinkedQueue<>();
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
    flush(r -> context.output(r.getValue(), r.getTimestamp(), r.getWindow()));
  }

  // kept for binary compatibility. Must not be used
  // TODO: remove in 0.15.0
  @Deprecated
  public void processElement(
      Input input, Instant timestamp, OutputReceiver<Output> out, BoundedWindow window) {
    processElement(input, timestamp, window, null, out);
  }

  @ProcessElement
  public void processElement(
      @Element Input element,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo pane,
      OutputReceiver<Output> out) {
    flush(
        r -> {
          final Output o = r.getValue();
          final Instant ts = r.getTimestamp();
          final Collection<BoundedWindow> ws = Collections.singleton(r.getWindow());
          final PaneInfo p = r.getPane();
          out.outputWindowedValue(o, ts, ws, p);
        });

    try {
      final UUID uuid = UUID.randomUUID();
      final Future future = processElement(element);
      futures.put(uuid, handleOutput(future, uuid, timestamp, window, pane));
    } catch (Exception e) {
      LOG.error("Failed to process element", e);
      throw e;
    }
  }

  private Future handleOutput(
      Future future, UUID key, Instant timestamp, BoundedWindow window, PaneInfo pane) {
    return addCallback(
        future,
        output -> {
          results.add(Pair.of(key, ValueInSingleWindow.of(output, timestamp, window, pane)));
          return null;
        },
        throwable -> {
          errors.add(throwable);
          return null;
        });
  }

  private void flush(Consumer<ValueInSingleWindow<Output>> outputFn) {
    if (!errors.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to process futures");
      Throwable t = errors.poll();
      while (t != null) {
        e.addSuppressed(t);
        t = errors.poll();
      }
      throw e;
    }
    Pair<UUID, ValueInSingleWindow<Output>> r = results.poll();
    while (r != null) {
      UUID key = r.getKey();
      outputFn.accept(r.getValue());
      futures.remove(key);
      r = results.poll();
    }
  }
}
