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

import com.google.bigtable.repackaged.com.google.cloud.grpc.BigtableClusterUtilities;
import com.google.bigtable.repackaged.com.google.com.google.bigtable.admin.v2.Cluster;
import com.google.cloud.bigtable.dataflow.CloudBigtableConfiguration;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.joda.time.Duration;

/**
 * Utilities to deal with Bigtable.
 */
public final class BigtableUtil {

  private BigtableUtil() { }

  /**
   * Deprecated. Please use {@link BigtableClusterUtilities#setClusterSize(String, String, int)}
   *
   * Updates all clusters within the specified Bigtable Instance to have a specified number
   * of nodes. Useful for increasing the number of nodes at the start of the job and decreasing
   * it at the end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param cloudBigtableConfiguration Configuration of Bigtable Instance
   * @param numberOfNodes Number of nodes to make cluster
   * @param sleepDuration How long to sleep after updating the number of nodes. Google recommends
   *                      at least 20 minutes before the new nodes are fully functional
   * @throws IOException If setting up channel pool fails
   * @throws InterruptedException If sleep fails
   */
  public static void updateNumberOfBigtableNodes(final CloudBigtableConfiguration cloudBigtableConfiguration,
                                                 final int numberOfNodes,
                                                 final Duration sleepDuration)
      throws IOException, GeneralSecurityException, InterruptedException {
    final BigtableClusterUtilities clusterUtilities =
        BigtableClusterUtilities.forInstance(cloudBigtableConfiguration.getProjectId(),
                                             cloudBigtableConfiguration.getInstanceId());

    for (Cluster cluster : clusterUtilities.getClusters().getClustersList()) {
      final String clusterId = extractAfterSlash(cluster.getName());
      final String zoneId = extractAfterSlash(cluster.getLocation());
      clusterUtilities.setClusterSize(clusterId, zoneId, numberOfNodes);
    }
    Thread.sleep(sleepDuration.getMillis());
  }

  private static String extractAfterSlash(final String path) {
    return path.substring(path.lastIndexOf("/") + 1);
  }
}
