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

package com.spotify.scio.testing.util

import java.io.IOException
import java.nio.file.FileAlreadyExistsException

import com.google.api.client.util.Sleeper
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.storage.model.Bucket
import com.google.cloud.hadoop.util.{ResilientOperation, RetryDeterminer}
import com.google.common.base.Strings.isNullOrEmpty
import org.apache.beam.sdk.extensions.gcp.options.{GcpOptions, GcsOptions}
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.util.gcsfs.GcsPath
import org.apache.beam.sdk.util.{BackOff, BackOffAdapter, FluentBackoff}
import org.joda.time.Duration
import org.slf4j.{Logger, LoggerFactory}

private object DefaultBucket {

  def tryCreateDefaultBucket(options: PipelineOptions, crmClient: CloudResourceManager): String = {
    val gcpOptions = options.as(classOf[GcsOptions])
    val projectId = gcpOptions.getProject
    require(!isNullOrEmpty(projectId), "--project is a required option.")
    // Look up the project number, to create a default bucket with a stable
    // name with no special characters.
    var projectNumber = 0L
    try
      projectNumber = getProjectNumber(projectId, crmClient)
    catch {
      case e: IOException =>
        throw new RuntimeException("Unable to verify project with ID " + projectId, e)
    }
    var region = DEFAULT_REGION
    if (!isNullOrEmpty(gcpOptions.getZone)) region = getRegionFromZone(gcpOptions.getZone)
    val bucketName = "dataflow-staging-" + region + "-" + projectNumber
    LOG.info("No staging location provided, attempting to use default bucket: {}", bucketName)
    val bucket = new Bucket().setName(bucketName).setLocation(region)
    // Always try to create the bucket before checking access, so that we do not
    // race with other pipelines that may be attempting to do the same thing.
    try
      gcpOptions.getGcsUtil.createBucket(projectId, bucket)
    catch {
      case e: FileAlreadyExistsException =>
        LOG.debug("Bucket '{}'' already exists, verifying access.", bucketName)
      case e: IOException =>
        throw new RuntimeException("Unable create default bucket.", e)
    }
    // Once the bucket is expected to exist, verify that it is correctly owned
    // by the project executing the job.
    try {
      val owner = gcpOptions.getGcsUtil.bucketOwner(GcsPath.fromComponents(bucketName, ""))
      require(owner == projectNumber,
        s"Bucket owner does not match the project from --project: $owner vs. $projectNumber")
    } catch {
      case e: IOException =>
        throw new RuntimeException("Unable to determine the owner of the default bucket at gs://" +
          bucketName, e)
    }
    "gs://" + bucketName
  }

  private val BACKOFF_FACTORY = FluentBackoff.DEFAULT.withMaxRetries(3)
    .withInitialBackoff(Duration.millis(200))

  private val DEFAULT_REGION = "us-central1"
  private val LOG: Logger = LoggerFactory.getLogger(classOf[GcpOptions.GcpTempLocationFactory])

  private def getProjectNumber(projectId: String, crmClient: CloudResourceManager): Long =
    getProjectNumber(projectId, crmClient, BACKOFF_FACTORY.backoff(), Sleeper.DEFAULT)

  private def getProjectNumber(projectId: String,
                               crmClient: CloudResourceManager,
                               backoff: BackOff,
                               sleeper: Sleeper): Long = {
    val getProject = crmClient.projects.get(projectId)
    try {
      val project = ResilientOperation.retry(
        ResilientOperation.getGoogleRequestCallable(getProject),
        BackOffAdapter.toGcpBackOff(backoff),
        RetryDeterminer.SOCKET_ERRORS,
        classOf[IOException],
        sleeper)
      project.getProjectNumber
    } catch {
      case e: Exception =>
        throw new IOException("Unable to get project number", e)
    }
  }

  private def getRegionFromZone(zone: String): String = {
    val zoneParts = zone.split("-")
    require(zoneParts.length >= 2, s"Invalid zone provided: $zone")
    zoneParts(0) + "-" + zoneParts(1)
  }
}
