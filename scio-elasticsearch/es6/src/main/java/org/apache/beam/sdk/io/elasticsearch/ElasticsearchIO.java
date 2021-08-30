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

package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchIO {

  public static class Write {

    private static final Logger LOG = LoggerFactory.getLogger(Write.class);
    private static final String RETRY_ATTEMPT_LOG =
        "Error writing to Elasticsearch. Retry attempt[%d]";
    private static final String RETRY_FAILED_LOG =
        "Error writing to ES after %d attempt(s). No more attempts allowed";

    /**
     * Returns a tranform for writing to Elasticsearch cluster for a given name.
     *
     * @param clusterName name of the Elasticsearch cluster
     */
    public static <T> Bound<T> withClusterName(String clusterName) {
      return new Bound<T>().withClusterName(clusterName);
    }

    /**
     * Returns a transform for writing to the Elasticsearch cluster for a given servers.
     *
     * @param servers endpoints for the Elasticsearch cluster
     */
    public static <T> Bound<T> withServers(InetSocketAddress[] servers) {
      return new Bound<T>().withServers(servers);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster by providing slight delay specified
     * by flushInterval.
     *
     * @param flushInterval delay applied to buffer elements. Defaulted to 1 seconds.
     */
    public static <T> Bound withFlushInterval(Duration flushInterval) {
      return new Bound<T>().withFlushInterval(flushInterval);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     *
     * @param function creates IndexRequest required by Elasticsearch client
     */
    public static <T> Bound withFunction(
        SerializableFunction<T, Iterable<DocWriteRequest<?>>> function) {
      return new Bound<T>().withFunction(function);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster. Note: Recommended to set this
     * number as number of workers in your pipeline.
     *
     * @param numOfShard to construct a batch to bulk write to Elasticsearch.
     */
    public static <T> Bound withNumOfShard(long numOfShard) {
      return new Bound<>().withNumOfShard(numOfShard);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     *
     * @param error applies given function if specified in case of Elasticsearch error with bulk
     *     writes. Default behavior throws IOException.
     */
    public static <T> Bound withError(ThrowingConsumer<BulkExecutionException> error) {
      return new Bound<>().withError(error);
    }

    public static <T> Bound withMaxBulkRequestSize(int maxBulkRequestSize) {
      return new Bound<>().withMaxBulkRequestSize(maxBulkRequestSize);
    }

    public static <T> Bound withMaxBulkRequestBytes(long maxBulkRequestBytes) {
      return new Bound<>().withMaxBulkRequestBytes(maxBulkRequestBytes);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     *
     * @param maxRetries Maximum number of retries to attempt for saving any single chunk of bulk
     *     requests to the Elasticsearch cluster.
     */
    public static <T> Bound withMaxRetries(int maxRetries) {
      return new Bound<>().withMaxRetries(maxRetries);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     *
     * @param retryPause Duration to wait between successive retry attempts.
     */
    public static <T> Bound withRetryPause(Duration retryPause) {
      return new Bound<>().withRetryPause(retryPause);
    }

    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {

      private static final int CHUNK_SIZE = 3000;

      // 5 megabytes - recommended as a sensible default payload size (see
      // https://www.elastic.co/guide/en/elasticsearch/reference/6.8/getting-started-index.html#getting-started-batch-processing)
      private static final long CHUNK_BYTES = 5L * 1024L * 1024L;

      private static final int DEFAULT_RETRIES = 3;
      private static final Duration DEFAULT_RETRY_PAUSE = Duration.millis(35000);

      private final String clusterName;
      private final InetSocketAddress[] servers;
      private final Duration flushInterval;
      private final SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests;
      private final long numOfShard;
      private final int maxBulkRequestSize;
      private final long maxBulkRequestBytes;
      private final int maxRetries;
      private final Duration retryPause;
      private final ThrowingConsumer<BulkExecutionException> error;

      private Bound(
          final String clusterName,
          final InetSocketAddress[] servers,
          final Duration flushInterval,
          final SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests,
          final long numOfShard,
          final int maxBulkRequestSize,
          final long maxBulkRequestBytes,
          int maxRetries,
          Duration retryPause,
          final ThrowingConsumer<BulkExecutionException> error) {
        this.clusterName = clusterName;
        this.servers = servers;
        this.flushInterval = flushInterval;
        this.toDocWriteRequests = toDocWriteRequests;
        this.numOfShard = numOfShard;
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.maxBulkRequestBytes = maxBulkRequestBytes;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
        this.error = error;
      }

      Bound() {
        this(
            null,
            null,
            null,
            null,
            0,
            CHUNK_SIZE,
            CHUNK_BYTES,
            DEFAULT_RETRIES,
            DEFAULT_RETRY_PAUSE,
            defaultErrorHandler());
      }

      public Bound<T> withClusterName(String clusterName) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withServers(InetSocketAddress[] servers) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withFlushInterval(Duration flushInterval) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withFunction(
          SerializableFunction<T, Iterable<DocWriteRequest<?>>> toIndexRequest) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toIndexRequest,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withNumOfShard(long numOfShard) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withError(ThrowingConsumer<BulkExecutionException> error) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withMaxBulkRequestSize(int maxBulkRequestSize) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withMaxBulkRequestBytes(long maxBulkRequestBytes) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withMaxRetries(int maxRetries) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      public Bound<T> withRetryPause(Duration retryPause) {
        return new Bound<>(
            clusterName,
            servers,
            flushInterval,
            toDocWriteRequests,
            numOfShard,
            maxBulkRequestSize,
            maxBulkRequestBytes,
            maxRetries,
            retryPause,
            error);
      }

      @Override
      public PDone expand(final PCollection<T> input) {
        checkNotNull(clusterName);
        checkNotNull(servers);
        checkNotNull(toDocWriteRequests);
        checkNotNull(flushInterval);
        checkArgument(numOfShard >= 0);
        checkArgument(maxBulkRequestSize > 0);
        checkArgument(maxBulkRequestBytes > 0);
        checkArgument(maxRetries >= 0);
        checkArgument(retryPause.getMillis() >= 0);
        if (numOfShard == 0) {
          input.apply(
              ParDo.of(
                  new ElasticsearchWriter<>(
                      clusterName,
                      servers,
                      maxBulkRequestSize,
                      maxBulkRequestBytes,
                      toDocWriteRequests,
                      error,
                      maxRetries,
                      retryPause)));
        } else {
          input
              .apply("Assign To Shard", ParDo.of(new AssignToShard<>(numOfShard)))
              .apply(
                  "Re-Window to Global Window",
                  Window.<KV<Long, T>>into(new GlobalWindows())
                      .triggering(
                          Repeatedly.forever(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(flushInterval)))
                      .discardingFiredPanes()
                      .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW))
              .apply(GroupByKey.create())
              .apply(
                  "Write to Elasticsearch",
                  ParDo.of(
                      new ElasticsearchShardWriter<>(
                          clusterName,
                          servers,
                          maxBulkRequestSize,
                          maxBulkRequestBytes,
                          toDocWriteRequests,
                          error,
                          maxRetries,
                          retryPause)));
        }
        return PDone.in(input.getPipeline());
      }
    }

    private static class AssignToShard<T> extends DoFn<T, KV<Long, T>> {

      private final long numOfShard;

      public AssignToShard(long numOfShard) {
        this.numOfShard = numOfShard;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        // assign this element to a random shard
        final long shard = ThreadLocalRandom.current().nextLong(numOfShard);
        c.output(KV.of(shard, c.element()));
      }
    }

    private static class ElasticsearchWriter<T> extends DoFn<T, Void> {

      private BulkRequest chunk;
      private long currentSize;
      private long currentBytes;

      private final ClientSupplier clientSupplier;
      private final SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests;
      private final ThrowingConsumer<BulkExecutionException> error;
      private final int maxBulkRequestSize;
      private final long maxBulkRequestBytes;
      private final int maxRetries;
      private final Duration retryPause;

      private ProcessFunction<BulkRequest, BulkResponse> requestFn;
      private ProcessFunction<BulkRequest, BulkResponse> retryFn;

      public ElasticsearchWriter(
          String clusterName,
          InetSocketAddress[] servers,
          int maxBulkRequestSize,
          long maxBulkRequestBytes,
          SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests,
          ThrowingConsumer<BulkExecutionException> error,
          int maxRetries,
          Duration retryPause) {
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.maxBulkRequestBytes = maxBulkRequestBytes;
        this.clientSupplier = new ClientSupplier(clusterName, servers);
        this.toDocWriteRequests = toDocWriteRequests;
        this.error = error;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
      }

      @Setup
      public void setup() throws Exception {
        final FluentBackoff backoffConfig =
            FluentBackoff.DEFAULT
                .withMaxRetries(this.maxRetries)
                .withInitialBackoff(this.retryPause);
        this.requestFn = request(clientSupplier, error);
        this.retryFn = retry(requestFn, backoffConfig);
      }

      @StartBundle
      public void startBundle(StartBundleContext c) {
        chunk = new BulkRequest();
        currentSize = 0;
        currentBytes = 0;
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        flush();
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        final T t = c.element();
        final Iterable<DocWriteRequest<?>> requests = toDocWriteRequests.apply(t);
        for (DocWriteRequest request : requests) {
          long requestBytes = documentSize(request);
          if (currentSize < maxBulkRequestSize
              && (currentBytes + requestBytes) < maxBulkRequestBytes) {
            chunk.add(request);
            currentSize += 1;
            currentBytes += requestBytes;
          } else {
            flush();
            chunk = new BulkRequest().add(request);
            currentSize = 1;
            currentBytes = requestBytes;
          }
        }
      }

      private void flush() throws Exception {
        if (chunk.numberOfActions() < 1) {
          return;
        }
        try {
          requestFn.apply(chunk);
        } catch (Exception e) {
          retryFn.apply(chunk);
        }
      }
    }

    private static class ElasticsearchShardWriter<T> extends DoFn<KV<Long, Iterable<T>>, Void> {

      private final ClientSupplier clientSupplier;
      private final SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests;
      private final ThrowingConsumer<BulkExecutionException> error;
      private final int maxBulkRequestSize;
      private final long maxBulkRequestBytes;
      private final int maxRetries;
      private final Duration retryPause;

      private ProcessFunction<BulkRequest, BulkResponse> requestFn;
      private ProcessFunction<BulkRequest, BulkResponse> retryFn;

      public ElasticsearchShardWriter(
          String clusterName,
          InetSocketAddress[] servers,
          int maxBulkRequestSize,
          long maxBulkRequestBytes,
          SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests,
          ThrowingConsumer<BulkExecutionException> error,
          int maxRetries,
          Duration retryPause) {
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.maxBulkRequestBytes = maxBulkRequestBytes;
        this.clientSupplier = new ClientSupplier(clusterName, servers);
        this.toDocWriteRequests = toDocWriteRequests;
        this.error = error;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
      }

      @Setup
      public void setup() throws Exception {
        final FluentBackoff backoffConfig =
            FluentBackoff.DEFAULT
                .withMaxRetries(this.maxRetries)
                .withInitialBackoff(this.retryPause);
        this.requestFn = request(clientSupplier, error);
        this.retryFn = retry(requestFn, backoffConfig);
      }

      @SuppressWarnings("Duplicates")
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        final Iterable<T> values = c.element().getValue();

        // Elasticsearch throws ActionRequestValidationException if bulk request is empty,
        // so do nothing if number of actions is zero.
        if (!values.iterator().hasNext()) {
          LOG.info("ElasticsearchWriter: no requests to send");
          return;
        }

        final Stream<DocWriteRequest> docWriteRequests =
            StreamSupport.stream(values.spliterator(), false)
                .map(toDocWriteRequests::apply)
                .flatMap(ar -> StreamSupport.stream(ar.spliterator(), false));

        int currentSize = 0;
        long currentBytes = 0L;
        BulkRequest chunk = new BulkRequest();

        for (DocWriteRequest request : (Iterable<DocWriteRequest>) docWriteRequests::iterator) {
          long requestBytes = documentSize(request);
          if (currentSize < maxBulkRequestSize
              && (currentBytes + requestBytes) < maxBulkRequestBytes) {
            chunk.add(request);
            currentSize += 1;
            currentBytes += requestBytes;
          } else {
            flush(chunk);
            chunk = new BulkRequest().add(request);
            currentSize = 1;
            currentBytes = requestBytes;
          }
        }

        flush(chunk);
      }

      private void flush(BulkRequest chunk) throws Exception {
        if (chunk.numberOfActions() < 1) {
          return;
        }

        try {
          requestFn.apply(chunk);
        } catch (Exception e) {
          retryFn.apply(chunk);
        }
      }
    }

    private static ProcessFunction<BulkRequest, BulkResponse> request(
        final ClientSupplier clientSupplier,
        final ThrowingConsumer<BulkExecutionException> bulkErrorHandler) {
      return chunk -> {
        final BulkResponse bulkItemResponse = clientSupplier.get().bulk(chunk).get();

        if (bulkItemResponse.hasFailures()) {
          bulkErrorHandler.accept(new BulkExecutionException(bulkItemResponse));
        }

        return bulkItemResponse;
      };
    }

    private static ProcessFunction<BulkRequest, BulkResponse> retry(
        final ProcessFunction<BulkRequest, BulkResponse> requestFn,
        final FluentBackoff backoffConfig) {
      return chunk -> {
        final BackOff backoff = backoffConfig.backoff();
        int attempt = 0;
        BulkResponse response = null;
        Exception exception = null;

        while (response == null && BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
          LOG.warn(String.format(RETRY_ATTEMPT_LOG, ++attempt));
          try {
            response = requestFn.apply(chunk);
            exception = null;
          } catch (Exception e) {
            exception = e;
          }
        }

        if (exception != null) {
          throw new Exception(String.format(RETRY_FAILED_LOG, attempt), exception);
        }

        return response;
      };
    }

    private static class ClientSupplier implements Supplier<Client>, Serializable {

      private final AtomicReference<Client> CLIENT = new AtomicReference<>();
      private final String clusterName;
      private final InetSocketAddress[] addresses;

      public ClientSupplier(final String clusterName, final InetSocketAddress[] addresses) {
        this.clusterName = clusterName;
        this.addresses = addresses;
      }

      @Override
      public Client get() {
        if (CLIENT.get() == null) {
          synchronized (CLIENT) {
            if (CLIENT.get() == null) {
              CLIENT.set(create(clusterName, addresses));
            }
          }
        }
        return CLIENT.get();
      }

      private TransportClient create(String clusterName, InetSocketAddress[] addresses) {
        final Settings settings = Settings.builder().put("cluster.name", clusterName).build();

        TransportAddress[] transportAddresses =
            Arrays.stream(addresses).map(TransportAddress::new).toArray(TransportAddress[]::new);

        return new PreBuiltTransportClient(settings).addTransportAddresses(transportAddresses);
      }
    }

    private static ThrowingConsumer<BulkExecutionException> defaultErrorHandler() {
      return throwable -> {
        throw throwable;
      };
    }

    /** An exception that puts information about the failures in the bulk execution. */
    public static class BulkExecutionException extends IOException {

      private final Iterable<Throwable> failures;

      BulkExecutionException(BulkResponse bulkResponse) {
        super(bulkResponse.buildFailureMessage());
        this.failures =
            Arrays.stream(bulkResponse.getItems())
                .map(BulkItemResponse::getFailure)
                .filter(Objects::nonNull)
                .map(BulkItemResponse.Failure::getCause)
                .collect(Collectors.toList());
      }

      public Iterable<Throwable> getFailures() {
        return failures;
      }
    }
  }

  private static long documentSize(DocWriteRequest request) {
    if (request instanceof IndexRequest) {
      return ((IndexRequest) request).source().length();
    } else if (request instanceof UpdateRequest) {
      return ((UpdateRequest) request).doc().source().length();
    } else if (request instanceof DeleteRequest) {
      return 0;
    }
    throw new IllegalArgumentException("Encountered unknown subclass of DocWriteRequest");
  }
}
