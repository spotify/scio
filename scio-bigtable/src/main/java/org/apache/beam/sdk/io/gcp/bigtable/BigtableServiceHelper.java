package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.cloud.bigtable.config.BigtableOptions;

public class BigtableServiceHelper extends BigtableServiceImpl {

  public BigtableServiceHelper(BigtableOptions options) {
    super(options);
  }
}
