package com.spotify.scio.elasticsearch;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
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
    public static Bound withClusterName(String clusterName) {
      return new Bound().withClusterName(clusterName);
    }

    /**
     * Returns a transform for writing to the Elasticsearch cluster for a given servers.
     *
     * @param servers endpoints for the Elasticsearch cluster
     */
    public static Bound withServers(InetSocketAddress[] servers) {
      return new Bound().withServers(servers);
    }

    /**
     * Returns a transform for writing to Elasticsearch cluster by providing
     * slight delay specified by flushInterval.
     *
     * @param flushInterval delay applied to buffer elements. Defaulted to 1 seconds.
     */
    public static Bound withFlushInterval(Duration flushInterval) {
      return new Bound().withFlushInterval(flushInterval);
    }

    public static class Bound extends PTransform<PCollection<IndexRequestWrapper>, PDone> {
      private final String clusterName;
      private final InetSocketAddress[] servers;
      private final Duration flushInterval;

      private Bound(final String clusterName,
                    final InetSocketAddress[] servers,
                    final Duration flushInterval) {
        this.clusterName = clusterName;
        this.servers = servers;
        this.flushInterval = flushInterval == null? Duration.ofSeconds(1L): flushInterval;
      }

      Bound() {
        this(null, null, null);
      }

      public Bound withClusterName(String clusterName) {
        return new Bound(clusterName, servers, flushInterval);
      }

      public Bound withServers(InetSocketAddress[] servers) {
        return new Bound(clusterName, servers, flushInterval);
      }

      public Bound withFlushInterval(Duration flushInterval) {
        return new Bound(clusterName, servers, flushInterval);
      }

      @Override
      public PDone expand(final PCollection<IndexRequestWrapper> input) {

        if (clusterName == null) {
          throw new IllegalStateException(
              "need to set clustername of ElasticsearchIO.Write transform");
        }

        if (servers == null) {
          throw new IllegalStateException(
              "need to set clustername of ElasticsearchIO.Write transform");
        }

        input
            .apply("Assign To Shard", ParDo.of(new AssignToShard()))
            .apply("Re-Window to Global Window", Window.<KV<Long, IndexRequestWrapper>>into(new GlobalWindows())
                       .triggering(Repeatedly.forever(
                           AfterProcessingTime
                               .pastFirstElementInPane()
                               .plusDelayOf(javaToJoda(flushInterval))))
                       .discardingFiredPanes())
            .apply(GroupByKey.create())
            .apply("Write to Elasticesarch", ParDo.of(new ElasticsearchWriter(clusterName, servers)));
        return PDone.in(input.getPipeline());
      }
      private org.joda.time.Duration javaToJoda(final Duration duration) {
        return duration == null ? null : org.joda.time.Duration.millis(duration.toMillis());
      }
    }

    private static class AssignToShard
        extends DoFn<IndexRequestWrapper, KV<Long, IndexRequestWrapper>> {
      private int numWorkers;

      @StartBundle
      public void startBundle(Context c) throws Exception {
        numWorkers = c
            .getPipelineOptions()
            .as(DataflowPipelineOptions.class)
            .getNumWorkers();
        // numWorkers will be zero when running ElasticsearchWriterIT. Set it
        // to 1 or ThreadLocalRandom's nextLong method will throw an exception.
        if (numWorkers == 0) {
          numWorkers = 1;
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        // assign this element to a random shard
        final long shard = ThreadLocalRandom.current().nextLong(2);
        c.output(KV.of(shard, c.element()));
      }
    }

    private static class ElasticsearchWriter
        extends DoFn<KV<Long, Iterable<IndexRequestWrapper>>, Void> {
      private final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);
      private final ClientSupplier clientSupplier;

      public ElasticsearchWriter(String clusterName,
                                 InetSocketAddress[] servers) {
        this.clientSupplier = new ClientSupplier(clusterName, servers);
      }
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        final BulkRequestBuilder bulkRequestBuilder = clientSupplier.get().prepareBulk();

        c.element().getValue().forEach(request -> bulkRequestBuilder.add(toIndexRequest(request)));
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

      private IndexRequest toIndexRequest(IndexRequestWrapper wrapper) {
        IndexRequest indexRequest = new IndexRequest(wrapper.index(), wrapper.type())
            .source(wrapper.source())
            .id(wrapper.id())
            .version(wrapper.ttl())
            .refresh(wrapper.refresh());

        if (wrapper.timestamp() != null)
          indexRequest.timestamp(wrapper.timestamp());

        if (wrapper.version() > 0 )
          indexRequest.version(wrapper.version());
        return indexRequest;
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
              return CLIENT.get();
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
