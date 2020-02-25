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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** A {@link DoFn} that downloads {@link URI} elements and processes them as local {@link Path}s. */
public class FileDownloadDoFn<OutputT> extends DoFn<URI, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(FileDownloadDoFn.class);

  private final List<Element> batch;
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

  @StartBundle
  public void startBundle() {
    this.batch.clear();
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    batch.add(new Element(c.element(), c.timestamp(), window));
    if (batch.size() >= batchSize) {
      processBatch(c);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    processBatch(c);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("Batch Size", batchSize))
        .add(DisplayData.item("Keep Downloaded Files", keep));
  }

  private void processBatch(ProcessContext c) {
    if (batch.isEmpty()) {
      return;
    }
    LOG.info("Processing batch of {}", batch.size());
    List<URI> uris = batch.stream().map(e -> e.uri).collect(Collectors.toList());
    remoteFileUtil.download(uris).stream().map(fn::apply).forEach(c::output);
    if (!keep) {
      LOG.info("Deleting batch of {}", batch.size());
      remoteFileUtil.delete(uris);
    }
    batch.clear();
  }

  private void processBatch(FinishBundleContext c) {
    if (batch.isEmpty()) {
      return;
    }
    LOG.info("Processing batch of {}", batch.size());
    List<URI> uris = batch.stream().map(e -> e.uri).collect(Collectors.toList());
    List<OutputT> outputs =
        remoteFileUtil.download(uris).stream().map(fn::apply).collect(Collectors.toList());
    // .forEach(c::output);
    Iterator<OutputT> i1 = outputs.iterator();
    Iterator<Element> i2 = batch.iterator();
    while (i1.hasNext() && i2.hasNext()) {
      Element e = i2.next();
      c.output(i1.next(), e.timestamp, e.window);
    }
    if (!keep) {
      LOG.info("Deleting batch of {}", batch.size());
      remoteFileUtil.delete(uris);
    }
    batch.clear();
  }

  private class Element {
    private URI uri;
    private Instant timestamp;
    private BoundedWindow window;

    Element(URI uri, Instant timestamp, BoundedWindow window) {
      this.uri = uri;
      this.timestamp = timestamp;
      this.window = window;
    }
  }
}
