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

package com.spotify.scio.bigtable

import java.util.UUID

import com.google.bigtable.v2.{Mutation, Row}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.BigtableClusterUtilities
import com.google.protobuf.ByteString
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import org.joda.time.Duration

object BigtableIT {
  val projectId = "data-integration-test"
  val instanceId = "scio-bigtable-it"
  val clusterId = "scio-bigtable-it-cluster"
  val zoneId = "us-east1-b"
  val tableId = "scio-bigtable-it-counts"
  val uuid = UUID.randomUUID()
  val testData = Seq((s"$uuid-key1", 1L), (s"$uuid-key2", 2L), (s"$uuid-key3", 3L))

  val bigtableOptions = new BigtableOptions.Builder()
    .setProjectId(projectId)
    .setInstanceId(instanceId)
    .build

  val FAMILY_NAME: String = "count"
  val COLUMN_QUALIFIER: ByteString = ByteString.copyFromUtf8("long")

  def toWriteMutation(key: String, value: Long): (ByteString, Iterable[Mutation]) = {
    val m = Mutations.newSetCell(
      FAMILY_NAME, COLUMN_QUALIFIER, ByteString.copyFromUtf8(value.toString), 0L)
    (ByteString.copyFromUtf8(key), Iterable(m))
  }

  def toDeleteMutation(key: String): (ByteString, Iterable[Mutation]) = {
    val m = Mutations.newDeleteFromRow
    (ByteString.copyFromUtf8(key), Iterable(m))
  }

  def fromRow(r: Row): (String, Long) =
    (r.getKey.toStringUtf8, r.getValue(FAMILY_NAME, COLUMN_QUALIFIER).get.toStringUtf8.toLong)
}

class BigtableIT extends PipelineSpec {

  import BigtableIT._

  "Update number of bigtable nodes" should "work" in {
    val channelPool = ChannelPoolCreator.createPool(bigtableOptions.getInstanceAdminHost)
    val bt = new BigtableClusterUtilities(bigtableOptions)
    val sc = ScioContext()
    sc.updateNumberOfBigtableNodes(projectId, instanceId, 4, Duration.standardSeconds(10))
    bt.getClusterNodeCount(clusterId, zoneId) shouldBe 4
    sc.updateNumberOfBigtableNodes(projectId, instanceId, 3, Duration.standardSeconds(10))
    bt.getClusterNodeCount(clusterId, zoneId) shouldBe 3
  }

  "BigtableIO" should "work" in {
    runWithContext{ sc =>
      sc.parallelize(testData.map(kv => toWriteMutation(kv._1, kv._2)))
        .saveAsBigtable(projectId, instanceId, tableId)
    }.waitUntilFinish()


    runWithContext { sc =>
      sc.bigtable(projectId, instanceId, tableId).map(fromRow) should containInAnyOrder(testData)
    }.waitUntilFinish()


    cleanup()
  }

  private def cleanup() = {
    runWithContext { sc =>
      sc.parallelize(testData.map(kv => toDeleteMutation(kv._1)))
        .saveAsBigtable(projectId, instanceId, tableId)
    }.waitUntilFinish()
  }
}
