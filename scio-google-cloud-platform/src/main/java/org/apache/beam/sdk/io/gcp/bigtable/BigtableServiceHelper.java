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

package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;

/** Wrap {@link BigtableServiceImpl} and expose package private methods. */
public class BigtableServiceHelper extends BigtableServiceImpl {
  private static final BigtableConfig EMPTY_CONFIG =
      BigtableConfig.builder().setValidate(true).build();

  public BigtableServiceHelper(BigtableOptions bigtableOptions, PipelineOptions pipelineOptions)
      throws IOException {
    super(translateToVeneerSettings(bigtableOptions, pipelineOptions));
  }

  public Writer openForWriting(String tableId) {
    BigtableWriteOptions options =
        BigtableWriteOptions.builder().setTableId(StaticValueProvider.of(tableId)).build();
    return openForWriting(options);
  }

  private static BigtableDataSettings translateToVeneerSettings(
      BigtableOptions bigtableOptions, PipelineOptions pipelineOptions) throws IOException {
    final BigtableConfig config =
        BigtableConfigTranslator.translateToBigtableConfig(EMPTY_CONFIG, bigtableOptions);
    return BigtableConfigTranslator.translateToVeneerSettings(config, pipelineOptions);
  }
}
