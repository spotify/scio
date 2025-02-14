package com.spotify.scio.bigtable

import com.google.cloud.bigtable.config.BigtableOptions

object BTOptions {
  def apply(projectId: String, instanceId: String): BigtableOptions =
    BigtableOptions.builder().setProjectId(projectId).setInstanceId(instanceId).build
}
