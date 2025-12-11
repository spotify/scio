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

import com.spotify.scio.util.RemoteFileUtil;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} that downloads {@link URI} elements and processes them as local {@link Path}s. */
public class FileDownloadDoFn<OutputT> extends DoFn<URI, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(FileDownloadDoFn.class);

  private final List<ValueInSingleWindow<URI>> batch;
  private final RemoteFileUtil remoteFileUtil;
  private final SerializableFunction<Path, OutputT> fn;
  private final int batchSize;
  private final boolean keep;

  /**
   * Create a new {@link FileDownloadDoFn} instance.
   *
   * @param remoteFileUtil {@link RemoteFileUtil} for downloading files.
   * @param fn function to process downloaded files.
   */
  public FileDownloadDoFn(RemoteFileUtil remoteFileUtil, SerializableFunction<Path, OutputT> fn) {
    this(remoteFileUtil, fn, 1, false);
  }

  /**
   * Create a new {@link FileDownloadDoFn} instance.
   *
   * @param remoteFileUtil {@link RemoteFileUtil} for downloading files.
   * @param fn function to process downloaded files.
   * @param batchSize batch size when downloading files.
   * @param keep keep downloaded files after processing.
   */
  public FileDownloadDoFn(
      RemoteFileUtil remoteFileUtil,
      SerializableFunction<Path, OutputT> fn,
      int batchSize,
      boolean keep) {
    this.remoteFileUtil = remoteFileUtil;
    this.fn = fn;
    this.batch = new ArrayList<>();
    this.batchSize = batchSize;
    this.keep = keep;
  }

  // Set to arbitrarily high value; required to preserve timestamp of original element
  // See: https://github.com/apache/beam/pull/34902#discussion_r2527777237
  @Override
  public @UnknownKeyFor @NonNull @Initialized Duration getAllowedTimestampSkew() {
    return Duration.standardDays(30);
  }

  @StartBundle
  public void startBundle(StartBundleContext context) {
    this.batch.clear();
  }

  @ProcessElement
  public void processElement(
      @DoFn.Element URI element,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo pane,
      OutputReceiver<OutputT> out) {
    batch.add(ValueInSingleWindow.of(element, timestamp, window, pane));
    if (batch.size() >= batchSize) {
      flush(
          r -> {
            final OutputT o = r.getValue();
            final Instant ts = r.getTimestamp();
            final Collection<BoundedWindow> ws = Collections.singleton(r.getWindow());
            final PaneInfo p = r.getPaneInfo();
            out.outputWindowedValue(o, ts, ws, p);
          });
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    flush(p -> context.output(p.getValue(), p.getTimestamp(), p.getWindow()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("Batch Size", batchSize))
        .add(DisplayData.item("Keep Downloaded Files", keep));
  }

  private void flush(Consumer<ValueInSingleWindow<OutputT>> outputFn) {
    if (batch.isEmpty()) {
      return;
    }
    LOG.info("Processing batch of {}", batch.size());
    List<URI> uris = batch.stream().map(ValueInSingleWindow::getValue).collect(Collectors.toList());
    List<Path> paths = remoteFileUtil.download(uris);

    Iterator<ValueInSingleWindow<URI>> inputIt = batch.iterator();
    Iterator<Path> pathIt = paths.iterator();
    while (inputIt.hasNext() && pathIt.hasNext()) {
      final ValueInSingleWindow<URI> r = inputIt.next();
      final Path path = pathIt.next();

      final OutputT o = fn.apply(path);
      final Instant ts = r.getTimestamp();
      final BoundedWindow w = r.getWindow();
      final PaneInfo p = r.getPaneInfo();
      outputFn.accept(ValueInSingleWindow.of(o, ts, w, p));
    }

    if (!keep) {
      LOG.info("Deleting batch of {}", batch.size());
      remoteFileUtil.delete(uris);
    }
    batch.clear();
  }
}
