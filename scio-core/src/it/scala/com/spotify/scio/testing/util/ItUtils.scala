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

import java.util.UUID

import com.google.api.client.http.HttpRequestInitializer
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer
import com.google.common.collect.ImmutableList
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.beam.sdk.extensions.gcp.options._
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.{RetryHttpRequestInitializer, Transport}

/** Integration test utilities. */
private[scio] object ItUtils {

  // XXX: Copied from BigQueryClient
  val PROJECT_KEY: String = "bigquery.project"

  /** Get project for integration test. */
  def project: String =
    if (sys.props(PROJECT_KEY) != null) {
      // this is usually set by CI script
      sys.props(PROJECT_KEY)
    } else {
      // fallback to local setting
      new DefaultProjectFactory().create(null)
    }

  /** Get GCP temp location for integration test. */
  def gcpTempLocation(prefix: String): String = {
    val opts = PipelineOptionsFactory.as(classOf[GcpOptions])
    opts.setProject(project)
    val bucket = DefaultBucket.tryCreateDefaultBucket(opts,
      newCloudResourceManagerClient(opts.as(classOf[CloudResourceManagerOptions]))
    )
    val uuid = UUID.randomUUID().toString
    s"$bucket/$prefix-$uuid"
  }

  private def newCloudResourceManagerClient(options: CloudResourceManagerOptions):
  CloudResourceManager = {
    val credentials = options.getGcpCredential
    if (credentials == null) {
      NullCredentialInitializer.throwNullCredentialException()
    }
    new CloudResourceManager.Builder(Transport.getTransport, Transport.getJsonFactory,
      chainHttpRequestInitializer(
        credentials,
        // Do not log 404. It clutters the output and is possibly even required by the caller.
        new RetryHttpRequestInitializer(ImmutableList.of(404))))
      .setApplicationName(options.getAppName)
      .setGoogleClientRequestInitializer(options.getGoogleApiTrace)
    .build()
  }

  private def chainHttpRequestInitializer(credential: Credentials,
                                          httpRequestInitializer: HttpRequestInitializer):
  HttpRequestInitializer = {
    if (credential == null) {
      new ChainingHttpRequestInitializer(new NullCredentialInitializer(), httpRequestInitializer)
    } else {
      new ChainingHttpRequestInitializer(new HttpCredentialsAdapter(credential),
        httpRequestInitializer);
    }
  }
}
