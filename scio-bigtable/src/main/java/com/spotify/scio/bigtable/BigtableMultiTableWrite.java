/*
 * Copyright 2016 Spotify AB.
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

import com.google.api.client.util.Lists;
import com.google.bigtable.repackaged.com.google.cloud.config.BulkOptions;
import com.google.cloud.bigtable.dataflow.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.dataflow.CloudBigtableConfiguration;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * <p>
 * Reimplemented utilities to create {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s for
 * writing to multiple Bigtable tables from within a dataflow pipeline.
 * </p>

 */
public class BigtableMultiTableWrite {

  /**
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of {@link KV}
   * of (String tableName, List of {@link Mutation}s) to the specified table. using the
   * BufferedMutator.
   */
  public static class CloudBigtableMultiTableBufferedWriteFn
          extends AbstractCloudBigtableTableDoFn<KV<String, Iterable<Mutation>>, Void> {
    private static final long serialVersionUID = 2L;
    private Map<String, BufferedMutator> mutators;

    // Stats
    private final Aggregator<Long, Long> mutationsCounter;
    private final Aggregator<Long, Long> exceptionsCounter;

    public CloudBigtableMultiTableBufferedWriteFn(CloudBigtableConfiguration config) {
      super(config);
      mutationsCounter = createAggregator("mutations", new Sum.SumLongFn());
      exceptionsCounter = createAggregator("exceptions", new Sum.SumLongFn());
    }

    @Override
    public void startBundle(Context context) throws Exception {
      mutators = Maps.newConcurrentMap();
    }

    private synchronized BufferedMutator getBufferedMutator(final Context context, final String tableName)
            throws IOException {
      BufferedMutator mutator = mutators.get(tableName);
      if (mutator == null) {
        BufferedMutator.ExceptionListener listener = createExceptionListener(context);
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                .writeBufferSize(BulkOptions.BIGTABLE_MAX_MEMORY_DEFAULT).listener(listener);
        mutator = getConnection().getBufferedMutator(params);
        mutators.put(tableName, mutator);
      }
      return mutator;
    }

    protected BufferedMutator.ExceptionListener createExceptionListener(final Context context) {
      return (exception, mutator) -> {
        logExceptions(context, exception);
        throw exception;
      };
    }

    /**
     * Performs an asynchronous mutation via {@link BufferedMutator#mutate(Mutation)}.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      KV<String, Iterable<Mutation>> element = context.element();
      final List<Mutation> mutations = Lists.newArrayList(element.getValue());
      getBufferedMutator(context, element.getKey()).mutate(mutations);
      mutationsCounter.addValue((long) mutations.size());
    }

    /**
     * Closes the {@link BufferedMutator} and {@link Connection}.
     */
    @Override
    public void finishBundle(Context context) throws Exception {
      try {
        for (BufferedMutator mutator : mutators.values()) {
          mutator.close();
        }
      } catch (RetriesExhaustedWithDetailsException exception) {
        exceptionsCounter.addValue((long) exception.getCauses().size());
        logExceptions(context, exception);
        rethrowException(exception);
      } finally {
        // Close the connection to clean up resources.
        super.finishBundle(context);
      }
    }
  }

  /**
   * Creates a {@link PTransform} that can write either a bounded or unbounded {@link PCollection}
   * of {@link KV} of (String tableName, List of {@link Mutation}s) to the specified table.
   *
   * <p>NOTE: This {@link PTransform} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences.  Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one.  In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   */
  public static PTransform<PCollection<KV<String, Iterable<Mutation>>>, PDone>
  writeToMultipleTables(CloudBigtableConfiguration config) {
    validateConfig(config);
    return new CloudBigtableIO.CloudBigtableWriteTransform<>(new CloudBigtableMultiTableBufferedWriteFn(config));
  }

  private static void checkNotNullOrEmpty(String value, String type) {
    checkArgument(
            !isNullOrEmpty(value), "A " + type + " must be set to configure Bigtable properly.");
  }

  private static void validateConfig(CloudBigtableConfiguration configuration) {
    checkNotNullOrEmpty(configuration.getProjectId(), "projectId");
    checkNotNullOrEmpty(configuration.getInstanceId(), "instanceId");
  }
}
