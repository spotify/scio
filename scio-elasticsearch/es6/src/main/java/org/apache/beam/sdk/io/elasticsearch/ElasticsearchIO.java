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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
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
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
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
     *              writes. Default behavior throws IOException.
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
     * requests to the Elasticsearch cluster.
     */
    public static <T> Bound withMaxRetries(int maxRetries) {
      return new Bound<>().withMaxRetries(maxRetries);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     *
     * @param retryPause Number of seconds to wait between successive retry attempts.
     */
    public static <T> Bound withRetryPause(int retryPause) {
      return new Bound<>().withRetryPause(retryPause);
    }

    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {

      private static final int CHUNK_SIZE = 3000;
      private static final int DEFAULT_RETRIES = 3;
      private static final int DEFAULT_RETRY_PAUSE = 5;

      private final String clusterName;
      private final InetSocketAddress[] servers;
      private final Duration flushInterval;
      private final SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests;
      private final long numOfShard;
      private final int maxBulkRequestSize;
      private final int maxRetries;
      private final int retryPause;
      private final ThrowingConsumer<BulkExecutionException> error;

      private Bound(final String clusterName,
          final InetSocketAddress[] servers,
          final Duration flushInterval,
          final SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests,
          final long numOfShard,
          final int maxBulkRequestSize,
          int maxRetries, int retryPause, final ThrowingConsumer<BulkExecutionException> error) {
        this.clusterName = clusterName;
        this.servers = servers;
        this.flushInterval = flushInterval;
        this.toDocWriteRequests = toDocWriteRequests;
        this.numOfShard = numOfShard;
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
        this.error = error;
      }

      Bound() {
        this(null, null, null, null, 0, CHUNK_SIZE, DEFAULT_RETRIES, DEFAULT_RETRY_PAUSE, defaultErrorHandler());
      }

      public Bound<T> withClusterName(String clusterName) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
                           maxBulkRequestSize, maxRetries, retryPause, error);
      }

      public Bound<T> withServers(InetSocketAddress[] servers) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
                           maxBulkRequestSize, maxRetries, retryPause, error);
      }

      public Bound<T> withFlushInterval(Duration flushInterval) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
                           maxBulkRequestSize, maxRetries, retryPause, error);
      }

      public Bound<T> withFunction(
          SerializableFunction<T, Iterable<DocWriteRequest<?>>> toIndexRequest) {
        return new Bound<>(clusterName, servers, flushInterval, toIndexRequest, numOfShard,
                           maxBulkRequestSize, maxRetries, retryPause, error);
      }

      public Bound<T> withNumOfShard(long numOfShard) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
                           maxBulkRequestSize, maxRetries, retryPause, error);
      }

      public Bound<T> withError(ThrowingConsumer<BulkExecutionException> error) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
                           maxBulkRequestSize, maxRetries, retryPause, error);
      }

      public Bound<T> withMaxBulkRequestSize(int maxBulkRequestSize) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
                           maxBulkRequestSize, maxRetries, retryPause, error);
      }

      public Bound<T> withMaxRetries(int maxRetries) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
            maxBulkRequestSize,
            maxRetries, retryPause, error);
      }

      public Bound<T> withRetryPause(int retryPause) {
        return new Bound<>(clusterName, servers, flushInterval, toDocWriteRequests, numOfShard,
            maxBulkRequestSize,
            maxRetries, retryPause, error);
      }

      @Override
      public PDone expand(final PCollection<T> input) {
        checkNotNull(clusterName);
        checkNotNull(servers);
        checkNotNull(toDocWriteRequests);
        checkNotNull(flushInterval);
        checkArgument(numOfShard > 0);
        checkArgument(maxBulkRequestSize > 0);
        checkArgument(maxRetries >= 0);
        checkArgument(retryPause >= 0);
        input
            .apply("Assign To Shard", ParDo.of(new AssignToShard<>(numOfShard)))
            .apply("Re-Window to Global Window", Window.<KV<Long, T>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(
                    AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(flushInterval)))
                .discardingFiredPanes()
                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW))
            .apply(GroupByKey.create())
            .apply("Write to Elasticsearch",
                   ParDo.of(new ElasticsearchWriter<>
                                (clusterName, servers, maxBulkRequestSize, toDocWriteRequests,
                                 error, maxRetries, retryPause)));
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

    private static class ElasticsearchWriter<T> extends DoFn<KV<Long, Iterable<T>>, Void> {

      private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);
      private final ClientSupplier clientSupplier;
      private final SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests;
      private final ThrowingConsumer<BulkExecutionException> error;
      private FluentBackoff backoffConfig;
      private final int maxBulkRequestSize;
      private final int maxRetries;
      private final int retryPause;

      public ElasticsearchWriter(String clusterName,
                    InetSocketAddress[] servers,
                    int maxBulkRequestSize,
                    SerializableFunction<T, Iterable<DocWriteRequest<?>>> toDocWriteRequests,
                    ThrowingConsumer<BulkExecutionException> error,
                    int maxRetries,
                    int retryPause) {
        this.maxBulkRequestSize = maxBulkRequestSize;
        this.clientSupplier = new ClientSupplier(clusterName, servers);
        this.toDocWriteRequests = toDocWriteRequests;
        this.error = error;
        this.maxRetries = maxRetries;
        this.retryPause = retryPause;
      }

      @Setup
      public void setup() throws Exception {
        this.backoffConfig = FluentBackoff.DEFAULT
            .withMaxRetries(this.maxRetries)
            .withInitialBackoff(Duration.standardSeconds(this.retryPause));
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

        final Iterable<List<DocWriteRequest>> chunks =
            Iterables.partition(docWriteRequests::iterator, maxBulkRequestSize);

        chunks.forEach(chunk -> {
          Exception exception;

          final BackOff backoff = backoffConfig.backoff();

          do {
            try {
              final BulkRequest bulkRequest = new BulkRequest().add(chunk.toArray(
                  new DocWriteRequest[0]))
                  .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
              final BulkResponse bulkItemResponse = clientSupplier.get().bulk(bulkRequest).get();
              if (bulkItemResponse.hasFailures()) {
                throw new BulkExecutionException(bulkItemResponse);
              } else {
                exception = null;
                break;
              }
            } catch (Exception e) {
              exception = e;

              LOG.error(
                  "ElasticsearchWriter: Failed to bulk save chunk of " +
                      Objects.toString(chunk.size()),
                  exception);

              // Backoff
              try {
                final long sleepTime = backoff.nextBackOffMillis();
                if (sleepTime == BackOff.STOP) {
                  break;
                }
                Thread.sleep(sleepTime);
              } catch (InterruptedException | IOException e1) {
                LOG.error("Interrupt during backoff", e1);
                break;
              }
            }
          } while (true);

          try {
            if (exception != null) {
              if (exception instanceof BulkExecutionException) {
                // This may result in no exception being thrown, depending on callback.
                error.accept((BulkExecutionException) exception);
              } else {
                throw exception;
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
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
        final Settings settings = Settings.builder()
            .put("cluster.name", clusterName)
            .build();

        TransportAddress[] transportAddresses = Arrays.stream(addresses)
            .map(TransportAddress::new)
            .toArray(TransportAddress[]::new);

        return new PreBuiltTransportClient(settings)
            .addTransportAddresses(transportAddresses);
      }
    }

    private static ThrowingConsumer<BulkExecutionException> defaultErrorHandler() {
      return throwable -> {
        throw throwable;
      };
    }

    /**
     * An exception that puts information about the failures in the bulk execution.
     */
    public static class BulkExecutionException extends IOException {

      private final Iterable<Throwable> failures;

      BulkExecutionException(BulkResponse bulkResponse) {
        super(bulkResponse.buildFailureMessage());
        this.failures = Arrays.stream(bulkResponse.getItems())
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
}
