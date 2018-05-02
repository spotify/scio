/*
 * Copyright 2018 Spotify AB.
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

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableServiceHelper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableBulkWriter
        extends PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableBulkWriter.class);

  private final BigtableOptions bigtableOptions;
  private final String tableName;
  private final int numOfShards;
  private final Duration flushInterval;

  public BigtableBulkWriter(final String tableName,
                            final BigtableOptions bigtableOptions,
                            final int numOfShards,
                            final Duration flushInterval) {
    this.bigtableOptions = bigtableOptions;
    this.tableName = tableName;
    this.numOfShards = numOfShards;
    this.flushInterval = flushInterval;
  }

  @Override
  public PDone expand(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
    createBulkShards(input, numOfShards, flushInterval)
        .apply("Bigtable BulkWrite", ParDo.of(new BigtableBulkWriterFn()));
    return PDone.in(input.getPipeline());
  }

  @VisibleForTesting
  static PCollection<Iterable<KV<ByteString, Iterable<Mutation>>>> createBulkShards(
      final PCollection<KV<ByteString, Iterable<Mutation>>> input, final int numOfShards,
      final Duration flushInterval) {
    return input
        .apply("Assign To Shard", ParDo.of(new AssignToShard(numOfShards)))
        .apply("Window", Window
            .<KV<Long, KV<ByteString, Iterable<Mutation>>>>into(new GlobalWindows())
            .triggering(Repeatedly.forever(
                AfterProcessingTime
                    .pastFirstElementInPane()
                    .plusDelayOf(flushInterval)))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.ZERO))
        .apply("Group By Shard", GroupByKey.create())
        .apply("Gets Mutations", ParDo
            .of(new DoFn<KV<Long, Iterable<KV<ByteString, Iterable<Mutation>>>>,
                Iterable<KV<ByteString, Iterable<Mutation>>>>() {
              @ProcessElement
              public void process(ProcessContext c) {
                c.output(c.element().getValue());
              }
            }));
  }

  private class BigtableBulkWriterFn extends
                                     DoFn<Iterable<KV<ByteString, Iterable<Mutation>>>, Void> {

    private BigtableServiceHelper.Writer bigtableWriter;
    private long recordsWritten;
    private final ConcurrentLinkedQueue<BigtableWriteException> failures;

    public BigtableBulkWriterFn() {
      this.failures = new ConcurrentLinkedQueue<>();
    }

    @StartBundle
    public void startBundle(StartBundleContext c) throws IOException {
      if (bigtableWriter == null) {
        bigtableWriter = new BigtableServiceHelper(bigtableOptions)
            .openForWriting(tableName);
      }
      recordsWritten = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      checkForFailures(failures);
      for (KV<ByteString, Iterable<Mutation>> r : c.element()) {
        bigtableWriter
            .writeRecord(r)
            .whenComplete(
                (mutationResult, exception) -> {
                  if (exception != null) {
                    failures.add(new BigtableWriteException(r, exception));
                  }
                });
        ++recordsWritten;
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      bigtableWriter.flush();
      checkForFailures(failures);
      LOG.debug("Wrote {} records", recordsWritten);
    }

    @Teardown
    public void tearDown() throws Exception {
      if (bigtableWriter != null) {
        bigtableWriter.close();
        bigtableWriter = null;
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("Records Written", recordsWritten));
    }

    /**
     * If any write has asynchronously failed, fail the bundle with a useful error.
     */
    private void checkForFailures(final ConcurrentLinkedQueue<BigtableWriteException> failures)
        throws IOException {
      // Note that this function is never called by multiple threads and is the only place that
      // we remove from failures, so this code is safe.
      if (failures.isEmpty()) {
        return;
      }

      StringBuilder logEntry = new StringBuilder();
      int i = 0;
      List<BigtableWriteException> suppressed = Lists.newArrayList();
      for (; i < 10 && !failures.isEmpty(); ++i) {
        BigtableWriteException exc = failures.remove();
        logEntry.append("\n").append(exc.getMessage());
        if (exc.getCause() != null) {
          logEntry.append(": ").append(exc.getCause().getMessage());
        }
        suppressed.add(exc);
      }
      String message =
          String.format(
              "At least %d errors occurred writing to Bigtable. First %d errors: %s",
              i + failures.size(),
              i,
              logEntry.toString());
      LOG.error(message);
      IOException exception = new IOException(message);
      for (BigtableWriteException e : suppressed) {
        exception.addSuppressed(e);
      }
      throw exception;
    }

    /**
     * An exception that puts information about the failed record being written in its message.
     */
    class BigtableWriteException extends IOException {

      public BigtableWriteException(final KV<ByteString, Iterable<Mutation>> record,
                                    Throwable cause) {
        super(
            String.format(
                "Error mutating row %s with mutations %s",
                record.getKey().toStringUtf8(),
                record.getValue()),
            cause);
      }
    }
  }

  static class AssignToShard extends DoFn<KV<ByteString, Iterable<Mutation>>,
      KV<Long, KV<ByteString, Iterable<Mutation>>>> {

    private final int numOfShards;

    AssignToShard(final int numOfShards) {
      this.numOfShards = numOfShards;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      // assign this element to a random shard
      final long shard = ThreadLocalRandom.current().nextLong(numOfShards);
      c.output(KV.of(shard, c.element()));
    }
  }
}
