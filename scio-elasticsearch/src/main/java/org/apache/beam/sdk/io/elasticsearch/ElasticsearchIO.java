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

package org.apache.beam.sdk.io.elasticsearch;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import com.google.common.collect.Iterables;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import com.twitter.jsr166e.ThreadLocalRandom;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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
     * Returns a transform for writing to Elasticsearch cluster by providing
     * slight delay specified by flushInterval.
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
        SerializableFunction<T, Iterable<ActionRequest>> function) {
      return new Bound<T>().withFunction(function);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     * Note: Recommended to set this number as number of workers in your pipeline.
     *
     * @param numOfShard to construct a batch to bulk write to Elasticsearch.
     */
    public static <T> Bound withNumOfShard(Long numOfShard) {
      return new Bound<>().withNumOfShard(numOfShard);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     *
     * @param error applies given function if specified in case of
     *              Elasticsearch error with bulk writes. Default behavior throws IOException.
     */
    public static <T> Bound withError(ThrowingConsumer<BulkExecutionException> error) {
      return new Bound<>().withError(error);
    }

    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {

      private final String clusterName;
      private final InetSocketAddress[] servers;
      private final Duration flushInterval;
      private final SerializableFunction<T, Iterable<ActionRequest>> toActionRequests;
      private final Long numOfShard;
      private final ThrowingConsumer<BulkExecutionException> error;

      private Bound(final String clusterName,
                    final InetSocketAddress[] servers,
                    final Duration flushInterval,
                    final SerializableFunction<T, Iterable<ActionRequest>> toActionRequests,
                    final Long numOfShard,
                    final ThrowingConsumer<BulkExecutionException> error) {
        this.clusterName = clusterName;
        this.servers = servers;
        this.flushInterval = flushInterval;
        this.toActionRequests = toActionRequests;
        this.numOfShard = numOfShard;
        this.error = error;
      }

      Bound() {
        this(null, null, null, null, null, defaultErrorHandler());
      }

      public Bound<T> withClusterName(String clusterName) {
        return new Bound<>(clusterName, servers, flushInterval, toActionRequests, numOfShard,
            error);
      }

      public Bound<T> withServers(InetSocketAddress[] servers) {
        return new Bound<>(clusterName, servers, flushInterval, toActionRequests, numOfShard,
            error);
      }

      public Bound<T> withFlushInterval(Duration flushInterval) {
        return new Bound<>(clusterName, servers, flushInterval, toActionRequests, numOfShard,
            error);
      }

      public Bound<T> withFunction(
          SerializableFunction<T, Iterable<ActionRequest>> toIndexRequest) {
        return new Bound<>(clusterName, servers, flushInterval, toIndexRequest, numOfShard, error);
      }

      public Bound<T> withNumOfShard(Long numOfShard) {
        return new Bound<>(clusterName, servers, flushInterval, toActionRequests, numOfShard,
            error);
      }

      public Bound<T> withError(ThrowingConsumer<BulkExecutionException> error) {
        return new Bound<>(clusterName, servers, flushInterval, toActionRequests, numOfShard,
            error);
      }

      @Override
      public PDone expand(final PCollection<T> input) {
        checkNotNull(clusterName);
        checkNotNull(servers);
        checkNotNull(toActionRequests);
        checkNotNull(numOfShard);
        checkNotNull(flushInterval);
        input
            .apply("Assign To Shard", ParDo.of(new AssignToShard<>(numOfShard)))
            .apply("Re-Window to Global Window", Window.<KV<Long, T>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(
                    AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(flushInterval)))
                .discardingFiredPanes())
            .apply(GroupByKey.create())
            .apply("Write to Elasticesarch",
                ParDo.of(new ElasticsearchWriter<>
                    (clusterName, servers, toActionRequests, error)));
        return PDone.in(input.getPipeline());
      }
    }

    private static class AssignToShard<T> extends DoFn<T, KV<Long, T>> {

      private final Long numOfShard;

      public AssignToShard(Long numOfShard) {
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

      private final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);
      private final ClientSupplier clientSupplier;
      private final SerializableFunction<T, Iterable<ActionRequest>> toActionRequests;
      private final ThrowingConsumer<BulkExecutionException> error;

      public ElasticsearchWriter(String clusterName,
                                 InetSocketAddress[] servers,
                                 SerializableFunction<T, Iterable<ActionRequest>> toActionRequests,
                                 ThrowingConsumer<BulkExecutionException> error) {
        this.clientSupplier = new ClientSupplier(clusterName, servers);
        this.toActionRequests = toActionRequests;
        this.error = error;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        final List<Iterable<ActionRequest>> actionRequests =
            StreamSupport.stream(c.element().getValue().spliterator(), false)
                .map(toActionRequests::apply)
                .collect(Collectors.toList());
        // Elasticsearch throws ActionRequestValidationException if bulk request is empty,
        // so do nothing if number of actions is zero.
        if (actionRequests.isEmpty()) {
          LOG.info("ElasticsearchWriter: no requests to send");
          return;
        }

        final Client client = clientSupplier.get();
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Iterables.concat(actionRequests)
            .forEach(x -> {
              try {
                addBulkRequest(client, bulkRequestBuilder, x);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
        final BulkResponse bulkItemResponse = bulkRequestBuilder.get();

        if (bulkItemResponse.hasFailures()) {
          error.accept(new BulkExecutionException(bulkItemResponse));
        }
      }
    }

    /**
     * Accepts a Client, a BulkRequestBuilder, and an ActionRequest.
     * Properly typecasts & prepares an ActionRequest. Adds it to the BulkRequestBuilder.
     * Return type is void.
     *
     * @param client        provides the ability to prepare requests for bulk execution
     * @param bulkRequest   maintains an ordered list of ActionRequests to later be executed in bulk
     * @param actionRequest the ActionRequest to be added to the bulkRequest
     */
    private static void addBulkRequest(Client client, BulkRequestBuilder bulkRequest,
                                       ActionRequest actionRequest) throws IOException {
      String simpleName = actionRequest.getClass().getSimpleName();
      XContentBuilder xContentBuilder;
      switch (simpleName) {
        case "IndexRequest":
          IndexRequest indexRequest = (IndexRequest) actionRequest;
          if (indexRequest.source() == null) {
            xContentBuilder = jsonBuilder().startObject().endObject();
          } else {
            xContentBuilder = jsonBuilder().map(indexRequest.sourceAsMap());
          }
          bulkRequest.add(
              client.prepareIndex(indexRequest.index().toLowerCase(), indexRequest.type(),
                  indexRequest.id())
                  .setSource(xContentBuilder)
          );
          break;
        case "UpdateRequest":
          UpdateRequest updateRequest = (UpdateRequest) actionRequest;
          if (updateRequest.doc() == null) {
            xContentBuilder = jsonBuilder().startObject().endObject();
          } else {
            xContentBuilder = jsonBuilder().map(updateRequest.doc().sourceAsMap());
          }
          bulkRequest.add(
              client.prepareUpdate(updateRequest.index().toLowerCase(), updateRequest.type(),
                  updateRequest.id())
                  .setDoc(xContentBuilder)
          );
          break;
        case "DeleteRequest":
          DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
          bulkRequest.add(client
              .prepareDelete(deleteRequest.index().toLowerCase(), deleteRequest.type(),
                  deleteRequest.id())
          );
          break;
        default:
          String message = String.format("Unsupported ActionRequest Type: %s.", simpleName);
          throw new IllegalArgumentException(message);
      }
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

      InetSocketTransportAddress[] transportAddresses = Arrays.stream(addresses)
          .map(InetSocketTransportAddress::new)
          .toArray(InetSocketTransportAddress[]::new);

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

