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

package com.spotify.scio.elasticsearch;

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
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
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
    public static<T> Bound<T> withServers(InetSocketAddress[] servers) {
      return new Bound<T>().withServers(servers);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster by providing
     * slight delay specified by flushInterval.
     *
     * @param flushInterval delay applied to buffer elements. Defaulted to 1 seconds.
     */
    public static<T> Bound withFlushInterval(Duration flushInterval) {
      return new Bound<T>().withFlushInterval(flushInterval);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     *
     * @param function creates IndexRequest required by Elasticsearch client
     */
    public static<T> Bound withFunction(SerializableFunction function) {
      return new Bound<T>().withFunction(function);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster.
     * Note: Recommended to set this number as number of workers in your pipeline.
     *
     * @param numOfShard to construct a batch to bulk write to Elasticsearch.
     */
    public static<T> Bound withNumOfShard(Long numOfShard) {
      return new Bound<>().withNumOfShard(numOfShard);
    }

    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      private final String clusterName;
      private final InetSocketAddress[] servers;
      private final Duration flushInterval;
      private final SerializableFunction<T, IndexRequest> function;
      private final Long numOfShard;

      private Bound(final String clusterName,
                    final InetSocketAddress[] servers,
                    final Duration flushInterval,
                    final SerializableFunction<T, IndexRequest> function,
                    final Long numOfShard) {
        this.clusterName = clusterName;
        this.servers = servers;
        this.flushInterval = flushInterval == null? Duration.ofSeconds(1L): flushInterval;
        this.function = function;
        this.numOfShard = numOfShard;
      }

      Bound() {
        this(null, null, null, null, null);
      }

      public Bound<T> withClusterName(String clusterName) {
        return new Bound<>(clusterName, servers, flushInterval, function, numOfShard);
      }

      public Bound<T> withServers(InetSocketAddress[] servers) {
        return new Bound<>(clusterName, servers, flushInterval, function, numOfShard);
      }

      public Bound<T> withFlushInterval(Duration flushInterval) {
        return new Bound<>(clusterName, servers, flushInterval, function, numOfShard);
      }

      public Bound<T> withFunction(SerializableFunction<T, IndexRequest> function) {
        return new Bound<>(clusterName, servers, flushInterval, function, numOfShard);
      }

      public Bound<T> withNumOfShard(Long numOfShard) {
        return new Bound<>(clusterName, servers, flushInterval, function, numOfShard);
      }

      @Override
      public PDone expand(final PCollection<T> input) {

        if (clusterName == null) {
          throw new IllegalStateException(
              "need to set clustername of ElasticsearchIO.Write transform");
        }

        if (servers == null) {
          throw new IllegalStateException(
              "need to set clustername of ElasticsearchIO.Write transform");
        }

        if (function == null) {
          throw new IllegalStateException(
              "need to set SerializableFunction<T, IndexRequest> of ElasticsearchIO.Write transform");
        }

        if (numOfShard == null) {
          throw new IllegalStateException(
              "need to set numOfShard of ElasticsearchIO.Write transform");
        }
        input
            .apply("Assign To Shard", ParDo.of(new AssignToShard<>(numOfShard)))
            .apply("Re-Window to Global Window", Window.<KV<Long, T>>into(new GlobalWindows())
                       .triggering(Repeatedly.forever(
                           AfterProcessingTime
                               .pastFirstElementInPane()
                               .plusDelayOf(javaToJoda(flushInterval))))
                       .discardingFiredPanes())
            .apply(GroupByKey.create())
            .apply("Write to Elasticesarch",
                   ParDo.of(new ElasticsearchWriter(clusterName, servers, function)));
        return PDone.in(input.getPipeline());
      }
      private org.joda.time.Duration javaToJoda(final Duration duration) {
        return duration == null ? null : org.joda.time.Duration.millis(duration.toMillis());
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
      private final SerializableFunction<T, IndexRequest> function;

      public ElasticsearchWriter(String clusterName,
                                 InetSocketAddress[] servers,
                                 SerializableFunction<T, IndexRequest> function) {
        this.clientSupplier = new ClientSupplier(clusterName, servers);
        this.function = function;
      }
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        final BulkRequestBuilder bulkRequestBuilder = clientSupplier.get().prepareBulk();

        c.element().getValue().forEach(x -> bulkRequestBuilder.add(function.apply(x)));
        // Elasticsearch throws ActionRequestValidationException if bulk request is empty,
        // so do nothing if number of actions is zero.
        if (bulkRequestBuilder.numberOfActions() == 0) {
          LOG.info("ElasticsearchWriter: no requests to send");
          return;
        }

        final BulkResponse bulkItemResponse = bulkRequestBuilder.get();
        if (bulkItemResponse.hasFailures()) {
          throw new IOException(bulkItemResponse.buildFailureMessage());
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
        final Settings settings = Settings.settingsBuilder()
            .put("cluster.name", clusterName)
            .build();

        InetSocketTransportAddress[] transportAddresses = Arrays.stream(addresses)
            .map(InetSocketTransportAddress::new)
            .toArray(InetSocketTransportAddress[]::new);

        return TransportClient.builder()
            .settings(settings)
            .build()
            .addTransportAddresses(transportAddresses);
      }
    }
  }
}
