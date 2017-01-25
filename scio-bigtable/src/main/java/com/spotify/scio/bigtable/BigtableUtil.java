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

import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.ListClustersRequest;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceGrpcClient;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * Utilities to deal with Bigtable.
 */
public final class BigtableUtil {

  private BigtableUtil() { }

  /**
   * Updates all clusters within the specified Bigtable Instance to have a specified number
   * of nodes. Useful for increasing the number of nodes at the start of the job and decreasing
   * it at the end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param bigtableOptions Bigtable Options
   * @param numberOfNodes Number of nodes to make cluster
   * @param sleepDuration How long to sleep after updating the number of nodes. Google recommends
   *                      at least 20 minutes before the new nodes are fully functional
   * @throws IOException If setting up channel pool fails
   * @throws InterruptedException If sleep fails
   */
  public static void updateNumberOfBigtableNodes(
      final BigtableOptions bigtableOptions,
      final int numberOfNodes,
      final Duration sleepDuration
  ) throws IOException, InterruptedException {
    final ChannelPool channelPool =
        ChannelPoolCreator.createPool(bigtableOptions.getInstanceAdminHost());

    try {
      final BigtableInstanceClient bigtableInstanceClient = new BigtableInstanceGrpcClient(channelPool);

      final String instanceName = bigtableOptions.getInstanceName().toString();

      // Fetch clusters in Bigtable instance
      final ListClustersRequest clustersRequest =
          ListClustersRequest.newBuilder().setParent(instanceName).build();
      final ListClustersResponse clustersResponse =
          bigtableInstanceClient.listCluster(clustersRequest);

      // For each cluster update the number of nodes
      for (Cluster cluster : clustersResponse.getClustersList()) {
        final Cluster updatedCluster = Cluster.newBuilder()
            .setName(cluster.getName())
            .setServeNodes(numberOfNodes)
            .build();
        bigtableInstanceClient.updateCluster(updatedCluster);
      }

      // Wait for the new nodes to be provisioned
      Thread.sleep(sleepDuration.getMillis());
    } finally {
      channelPool.shutdownNow();
    }
  }
}
