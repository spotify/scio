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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Wrap {@link BigQueryServices} and expose package private methods.
 */
public class BigQueryServicesWrapper {

  private final BigQueryServices bqServices;
  private final BigQueryOptions bqOptions;

  public BigQueryServicesWrapper(BigQueryOptions bqOptions) {
    this.bqServices = new BigQueryServicesImpl();
    this.bqOptions = bqOptions;
  }

  public void createTable(TableReference ref, TableSchema schema)
      throws IOException, InterruptedException {
    Table table = new Table()
        .setTableReference(ref)
        .setSchema(schema);
    bqServices.getDatasetService(bqOptions).createTable(table);
  }

  public long insertAll(TableReference ref, List<TableRow> rowList)
      throws IOException, InterruptedException {
    List<ValueInSingleWindow<TableRow>> rows = rowList.stream()
        .map(r ->
            ValueInSingleWindow.of(
                r,
                BoundedWindow.TIMESTAMP_MIN_VALUE,
                GlobalWindow.INSTANCE,
                PaneInfo.NO_FIRING))
        .collect(Collectors.toList());
    return bqServices.getDatasetService(bqOptions)
        .insertAll(ref, rows, null, InsertRetryPolicy.alwaysRetry(), null);
  }

}
