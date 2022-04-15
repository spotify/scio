/*
 * Copyright 2022 Spotify AB.
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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.SimpleJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
     * Returns a transform for writing to the Elasticsearch cluster for the given nodes.
     *
     * @param nodes addresses for the Elasticsearch cluster
     */
    public static <T> Bound<T> withNodes(HttpHost[] nodes) {
      return new Bound<T>().withNodes(nodes);
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
        SerializableFunction<T, Iterable<BulkOperation>> function) {
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

    public static <T> Bound withCredentials(UsernamePasswordCredentials credentials) {
      return new Bound<>().withCredentials(credentials);
    }

    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {

      private static final int CHUNK_SIZE = 3000;

      private static final int DEFAULT_RETRIES = 3;
      private static final Duration DEFAULT_RETRY_PAUSE = Duration.millis(35000);

      private final HttpHost[] nodes;
      private final Duration flushInterval;
      private final SerializableFunction<T, Iterable<BulkOperation>> toBulkOperations;
      private final long numOfShard;
      private final int maxBulkRequestSize;
      private final int maxRetries;
      private final Duration retryPause;
      private final ThrowingConsumer<BulkExecutionException> error;
      private final UsernamePasswordCredentials credentials;

      private final JsonpMapperFactory mapperFactory;

      private Bound(
          final HttpHost[] nodes,
          final Duration flushInterval,
          final SerializableFunction<T, Iterable<BulkOperation>> toBulkOperations,
          final long numOfShard,
          final int maxBulkRequestSize,
          final int maxRetries,
          final Duration retryPause,
          final ThrowingConsumer<BulkExecutionException> error,
          final UsernamePasswordCredentials credentials,
          final JsonpMapperFactory mapperFactory) {
        this.nodes = nodes;
        this.flushInterval = flushInterval;
        this.toBulkOperations = toBulkOperations;
        this.numOfShard = numOfShard;
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
        this.error = error;
        this.credentials = credentials;
        this.mapperFactory = mapperFactory;
      }

      Bound() {
        this(
            null,
            null,
            null,
            0,
            CHUNK_SIZE,
            DEFAULT_RETRIES,
            DEFAULT_RETRY_PAUSE,
            defaultErrorHandler(),
            null,
            SimpleJsonpMapper::new);
      }

      public Bound<T> withNodes(HttpHost[] nodes) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withFlushInterval(Duration flushInterval) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withFunction(
          SerializableFunction<T, Iterable<BulkOperation>> toBulkOperations) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withNumOfShard(long numOfShard) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withError(ThrowingConsumer<BulkExecutionException> error) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withMaxBulkRequestSize(int maxBulkRequestSize) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withMaxRetries(int maxRetries) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withRetryPause(Duration retryPause) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withCredentials(UsernamePasswordCredentials credentials) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      public Bound<T> withMapperFactory(JsonpMapperFactory mapperFactory) {
        return new Bound<>(
            nodes,
            flushInterval,
            toBulkOperations,
            numOfShard,
            maxBulkRequestSize,
            maxRetries,
            retryPause,
            error,
            credentials,
            mapperFactory);
      }

      @Override
      public PDone expand(final PCollection<T> input) {
        checkNotNull(nodes);
        checkNotNull(toBulkOperations);
        checkNotNull(flushInterval);
        checkNotNull(mapperFactory);
        checkArgument(numOfShard >= 0);
        checkArgument(maxBulkRequestSize > 0);
        checkArgument(maxRetries >= 0);
        checkArgument(retryPause.getMillis() >= 0);
        if (numOfShard == 0) {
          input.apply(
              ParDo.of(
                  new ElasticsearchWriter<>(
                      nodes,
                      maxBulkRequestSize,
                      toBulkOperations,
                      error,
                      maxRetries,
                      retryPause,
                      credentials,
                      mapperFactory)));
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
                          nodes,
                          maxBulkRequestSize,
                          toBulkOperations,
                          error,
                          maxRetries,
                          retryPause,
                          credentials,
                          mapperFactory)));
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

      private List<BulkOperation> chunk;
      private final ClientSupplier clientSupplier;
      private final SerializableFunction<T, Iterable<BulkOperation>> toBulkOperations;
      private final ThrowingConsumer<BulkExecutionException> error;
      private final int maxBulkRequestSize;
      private final int maxRetries;
      private final Duration retryPause;

      private ProcessFunction<BulkRequest, BulkResponse> requestFn;
      private ProcessFunction<BulkRequest, BulkResponse> retryFn;

      public ElasticsearchWriter(
          HttpHost[] nodes,
          int maxBulkRequestSize,
          SerializableFunction<T, Iterable<BulkOperation>> toBulkOperations,
          ThrowingConsumer<BulkExecutionException> error,
          int maxRetries,
          Duration retryPause,
          UsernamePasswordCredentials credentials,
          JsonpMapperFactory mapperFactory) {
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.clientSupplier = new ClientSupplier(nodes, credentials, mapperFactory);
        this.toBulkOperations = toBulkOperations;
        this.error = error;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
      }

      @Setup
      public void setup() throws Exception {
        checkArgument(
            this.clientSupplier.get().ping().value(), "Elasticsearch client not reachable");

        final FluentBackoff backoffConfig =
            FluentBackoff.DEFAULT
                .withMaxRetries(this.maxRetries)
                .withInitialBackoff(this.retryPause);
        this.requestFn = request(clientSupplier, error);
        this.retryFn = retry(requestFn, backoffConfig);
      }

      @Teardown
      public void teardown() throws Exception {
        this.clientSupplier.getTransport().close();
      }

      @StartBundle
      public void startBundle(StartBundleContext c) {
        chunk = new ArrayList<>();
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        flush();
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        final T t = c.element();
        final Iterable<BulkOperation> operations = toBulkOperations.apply(t);
        for (BulkOperation operation : operations) {
          if (chunk.size() < maxBulkRequestSize) {
            chunk.add(operation);
          } else {
            flush();
            chunk.add(operation);
          }
        }
      }

      private void flush() throws Exception {
        if (chunk.isEmpty()) {
          return;
        }
        final BulkRequest request = BulkRequest.of(b -> b.operations(chunk));
        try {
          requestFn.apply(request);
        } catch (Exception e) {
          retryFn.apply(request);
        }
        chunk.clear();
      }
    }

    private static class ElasticsearchShardWriter<T> extends DoFn<KV<Long, Iterable<T>>, Void> {

      private final ClientSupplier clientSupplier;
      private final SerializableFunction<T, Iterable<BulkOperation>> toBulkOperations;
      private final ThrowingConsumer<BulkExecutionException> error;
      private final int maxBulkRequestSize;
      private final int maxRetries;
      private final Duration retryPause;

      private ProcessFunction<BulkRequest, BulkResponse> requestFn;
      private ProcessFunction<BulkRequest, BulkResponse> retryFn;

      public ElasticsearchShardWriter(
          HttpHost[] nodes,
          int maxBulkRequestSize,
          SerializableFunction<T, Iterable<BulkOperation>> toBulkOperations,
          ThrowingConsumer<BulkExecutionException> error,
          int maxRetries,
          Duration retryPause,
          UsernamePasswordCredentials credentials,
          JsonpMapperFactory mapperFactory) {
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.clientSupplier = new ClientSupplier(nodes, credentials, mapperFactory);
        this.toBulkOperations = toBulkOperations;
        this.error = error;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
      }

      @Setup
      public void setup() throws Exception {
        checkArgument(
            this.clientSupplier.get().ping().value(), "Elasticsearch client not reachable");

        final FluentBackoff backoffConfig =
            FluentBackoff.DEFAULT
                .withMaxRetries(this.maxRetries)
                .withInitialBackoff(this.retryPause);
        this.requestFn = request(clientSupplier, error);
        this.retryFn = retry(requestFn, backoffConfig);
      }

      @Teardown
      public void teardown() throws Exception {
        this.clientSupplier.getTransport().close();
      }

      @SuppressWarnings("Duplicates")
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        final Iterable<T> values = c.element().getValue();

        final Iterable<BulkOperation> operations =
            () ->
                StreamSupport.stream(values.spliterator(), false)
                    .map(toBulkOperations::apply)
                    .flatMap(ar -> StreamSupport.stream(ar.spliterator(), false))
                    .iterator();

        List<BulkOperation> chunk = new ArrayList<>();
        for (BulkOperation operation : operations) {
          if (chunk.size() < maxBulkRequestSize) {
            chunk.add(operation);
          } else {
            flush(chunk);
            chunk.add(operation);
          }
        }

        flush(chunk);
      }

      private void flush(List<BulkOperation> chunk) throws Exception {
        if (chunk.isEmpty()) {
          return;
        }
        final BulkRequest request = BulkRequest.of(r -> r.operations(chunk));
        try {
          requestFn.apply(request);
        } catch (Exception e) {
          retryFn.apply(request);
        }
        chunk.clear();
      }
    }

    private static ProcessFunction<BulkRequest, BulkResponse> request(
        final ClientSupplier clientSupplier,
        final ThrowingConsumer<BulkExecutionException> bulkErrorHandler) {
      return chunk -> {
        final BulkResponse bulkItemResponse = clientSupplier.get().bulk(chunk);

        if (bulkItemResponse.errors()) {
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

    private static class ClientSupplier implements Supplier<ElasticsearchClient>, Serializable {

      private final AtomicReference<RestClientTransport> transport = new AtomicReference<>();
      private final HttpHost[] nodes;
      private final UsernamePasswordCredentials credentials;
      private final JsonpMapperFactory mapperFactory;

      public ClientSupplier(
          final HttpHost[] nodes,
          final UsernamePasswordCredentials credentials,
          final JsonpMapperFactory mapperFactory) {
        this.nodes = nodes;
        this.credentials = credentials;
        this.mapperFactory = mapperFactory;
      }

      @Override
      public ElasticsearchClient get() {
        return new ElasticsearchClient(getTransport());
      }

      public RestClientTransport getTransport() {
        return transport.updateAndGet(c -> c == null ? createTransport() : c);
      }

      private RestClientTransport createTransport() {
        final RestClientBuilder builder = RestClient.builder(nodes);
        if (credentials != null) {
          final CredentialsProvider provider = new BasicCredentialsProvider();
          provider.setCredentials(AuthScope.ANY, credentials);
          builder.setHttpClientConfigCallback(
              asyncBuilder -> asyncBuilder.setDefaultCredentialsProvider(provider));
        }
        final RestClient restClient = builder.build();
        return new RestClientTransport(restClient, mapperFactory.create());
      }
    }

    private static ThrowingConsumer<BulkExecutionException> defaultErrorHandler() {
      return throwable -> {
        throw throwable;
      };
    }

    /** An exception that puts information about the failures in the bulk execution. */
    public static class BulkExecutionException extends IOException {

      private final Iterable<ErrorCause> failures;

      BulkExecutionException(BulkResponse bulkResponse) {
        super(buildFailureMessage(bulkResponse));
        this.failures = buildFailures(bulkResponse);
      }

      public Iterable<ErrorCause> getFailures() {
        return failures;
      }

      private static Iterable<ErrorCause> buildFailures(BulkResponse bulkResponse) {
        return bulkResponse.items().stream()
            .map(BulkResponseItem::error)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
      }

      private static String buildFailureMessage(BulkResponse bulkResponse) {
        final StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution:");
        for (BulkResponseItem item : bulkResponse.items()) {
          final ErrorCause cause = item.error();
          if (cause != null) {
            sb.append("\n[")
                .append(item)
                .append("]: index [")
                .append(item.index())
                .append("], type [")
                .append(item.operationType())
                .append("], id [")
                .append(item.id())
                .append("], message [")
                .append(cause.reason())
                .append("]");
          }
        }
        return sb.toString();
      }
    }
  }
}
