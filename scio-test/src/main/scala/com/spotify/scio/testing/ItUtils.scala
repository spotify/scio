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

package com.spotify.scio.testing

import java.util.UUID

import com.spotify.scio.bigquery.BigQueryClient
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.beam.sdk.extensions.gcp.options.{GcpOptions, GcsOptions}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.GcsUtil

/** Integration test utilities. */
private[scio] object ItUtils {

  /** Get project for integration test. */
  def project: String =
    if (sys.props(BigQueryClient.PROJECT_KEY) != null) {
      // this is usually set by CI script
      sys.props(BigQueryClient.PROJECT_KEY)
    } else {
      // fallback to local setting
      new DefaultProjectFactory().create(null)
    }

  /** Get [[org.apache.beam.sdk.util.GcsUtil GcsUtil]] for integration test. */
  def gcsUtil: GcsUtil = {
    val opts = PipelineOptionsFactory.as(classOf[GcsOptions])
    opts.setProject(project)
    opts.getGcsUtil
  }

  /** Get GCP temp location for integration test. */
  def gcpTempLocation(prefix: String): String = {
    val opts = PipelineOptionsFactory.as(classOf[GcpOptions])
    opts.setProject(project)
    val bucket = DefaultBucket.tryCreateDefaultBucket(opts)
    val uuid = UUID.randomUUID().toString
    s"$bucket/$prefix-$uuid"
  }

}
