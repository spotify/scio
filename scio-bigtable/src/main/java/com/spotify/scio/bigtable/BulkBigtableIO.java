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
import com.google.protobuf.ByteString;
import java.util.concurrent.ThreadLocalRandom;
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
import org.joda.time.Duration;

public class BulkBigtableIO {

  public static class Write {

    public static Bound withProjectId(String projectId) {
      return new Bound().withProjectId(projectId);
    }

    public static Bound withInstanceId(String instanceId) {
      return new Bound().withInstanceId(instanceId);
    }

    public static Bound withTableId(String tableId) {
      return new Bound().withTableId(tableId);
    }

    public static Bound withFlushInterval(Duration flushInterval) {
      return new Bound().withFlushInterval(flushInterval);
    }

    public static Bound withNumOfShards(int numOfShards) {
      return new Bound().withNumOfShards(numOfShards);
    }

    public static Bound withBigtableOptions(BigtableOptions bigtableOption) {
      return new Bound().withBigtableOptions(bigtableOption);
    }

    public static class Bound
        extends PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {

      private final String projectId;
      private final String instanceId;
      private final String tableId;
      private final Duration flushInterval;
      private final int numOfShards;
      private final BigtableOptions bigtableOption;

      private Bound(String projectId,
                    String instanceId,
                    String tableId,
                    BigtableOptions bigtableOption,
                    Duration flushInterval,
                    int numOfShards) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.flushInterval = flushInterval;
        this.numOfShards = numOfShards;
        this.bigtableOption = bigtableOption;
      }

      Bound() {
        this(null, null, null, null, Duration.standardSeconds(1), 1);
      }

      public Bound withProjectId(String projectId) {
        return new Bound(projectId, instanceId, tableId, bigtableOption, flushInterval,
                         numOfShards);
      }

      public Bound withInstanceId(String instanceId) {
        return new Bound(projectId, instanceId, tableId, bigtableOption, flushInterval,
                         numOfShards);
      }

      public Bound withTableId(String table) {
        return new Bound(projectId, instanceId, table, bigtableOption, flushInterval,
                         numOfShards);
      }

      public Bound withFlushInterval(Duration flushInterval) {
        return new Bound(projectId, instanceId, tableId, bigtableOption, flushInterval,
                         numOfShards);
      }

      public Bound withNumOfShards(int numOfShards) {
        return new Bound(projectId, instanceId, tableId, bigtableOption, flushInterval,
                         numOfShards);
      }

      public Bound withConfigurator(
          SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>
              configurator) {
        return new Bound(projectId, instanceId, tableId, bigtableOption, flushInterval,
                         numOfShards);
      }

      public Bound withBigtableOptions(BigtableOptions bigtableOption) {
        return new Bound(projectId, instanceId, tableId, bigtableOption, flushInterval,
                         numOfShards);
      }

      @Override
      public PDone expand(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
        return input
            .apply("Assign To Shard", ParDo.of(new AssignToShard(numOfShards)))
            .apply("Window", Window
                .<KV<Long, KV<ByteString, Iterable<Mutation>>>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(
                    AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(flushInterval)))
                .discardingFiredPanes())
            .apply("Group By Shard", GroupByKey.create())
            .apply("Gets Mutations", ParDo
                .of(new DoFn<KV<Long, Iterable<KV<ByteString, Iterable<Mutation>>>>,
                    Iterable<KV<ByteString, Iterable<Mutation>>>>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    c.output(c.element().getValue());
                  }
                }))
            .apply("Bigtable BulkWrite", BigtableIO.bulkWrite()
                .withProjectId(projectId)
                .withInstanceId(instanceId)
                .withTableId(tableId)
                .withBigtableOptions(bigtableOption));
      }
    }
  }

  static class AssignToShard extends DoFn<KV<ByteString, Iterable<Mutation>>,
      KV<Long, KV<ByteString, Iterable<Mutation>>>> {

    private final int numOfShards;

    AssignToShard(int numOfShards) {
      this.numOfShards = numOfShards;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) {
      // assign this element to a random shard
      final long shard = ThreadLocalRandom.current().nextLong(numOfShards);
      c.output(KV.of(shard, c.element()));
    }
  }
}
