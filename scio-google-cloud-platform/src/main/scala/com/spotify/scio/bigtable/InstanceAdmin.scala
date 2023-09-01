package com.spotify.scio.bigtable

import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.models.Cluster
import com.google.cloud.bigtable.beam.CloudBigtableConfiguration
import com.google.cloud.bigtable.hbase.BigtableConfiguration
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings
import org.apache.hadoop.hbase.client.AbstractBigtableConnection
import org.joda.time.Duration
import org.joda.time.format.PeriodFormatterBuilder
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._
import scala.util.chaining._
object InstanceAdmin {

  private val Formatter = new PeriodFormatterBuilder().appendDays
    .appendSuffix("d")
    .appendHours
    .appendSuffix("h")
    .appendMinutes
    .appendSuffix("m")
    .appendSeconds
    .appendSuffix("s")
    .toFormatter

  private val log: Logger = LoggerFactory.getLogger(TableAdmin.getClass)

  private def execute[A](
    config: CloudBigtableConfiguration
  )(f: BigtableInstanceAdminClient => A): A = {
    val connection = BigtableConfiguration.connect(config.toHBaseConfig)
    try {
      val settings = connection
        .asInstanceOf[AbstractBigtableConnection]
        .getBigtableSettings
        .asInstanceOf[BigtableHBaseVeneerSettings]
        .getInstanceAdminSettings

      f(BigtableInstanceAdminClient.create(settings))
    } finally {
      connection.close()
    }
  }

  def getCluster(config: CloudBigtableConfiguration, clusterId: String): Cluster =
    execute(config)(_.getCluster(config.getInstanceId, clusterId))

  def listClusters(config: CloudBigtableConfiguration): Iterable[Cluster] =
    execute(config)(_.listClusters(config.getInstanceId).asScala)

  def resizeClusters(
    config: CloudBigtableConfiguration,
    numServeNodes: Int,
    sleepDuration: Duration
  ): Unit = resizeClusters(config, None, numServeNodes, sleepDuration)

  def resizeClusters(
    config: CloudBigtableConfiguration,
    clusterIds: Set[String],
    numServeNodes: Int,
    sleepDuration: Duration
  ): Unit = resizeClusters(config, Some(clusterIds), numServeNodes, sleepDuration)

  private def resizeClusters(
    config: CloudBigtableConfiguration,
    clusterIds: Option[Set[String]],
    numServeNodes: Int,
    sleepDuration: Duration
  ): Unit =
    execute(config) { client =>
      client
        .listClusters(config.getInstanceId)
        .asScala
        .pipe(cs => clusterIds.fold(cs)(ids => cs.filter(c => ids.contains(c.getId))))
        .foreach { c =>
          // For each cluster update the number of nodes
          log.info("Updating number of nodes to {} for cluster {}", numServeNodes, c.getId);
          client.resizeCluster(c.getInstanceId, c.getId, numServeNodes)
        }

      // Wait for the new nodes to be provisioned
      if (sleepDuration.isLongerThan(Duration.ZERO)) {
        log.info("Sleeping for {} after update", Formatter.print(sleepDuration.toPeriod()));
        Thread.sleep(sleepDuration.getMillis);
      }
    }
}
