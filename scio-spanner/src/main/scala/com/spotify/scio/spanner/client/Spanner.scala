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

package com.spotify.scio.spanner.client

import com.google.cloud.spanner._
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig

object Spanner {
  private lazy val defaultInstance: Spanner = {
    SpannerOptions.newBuilder().build().getService
  }

  def databaseClient(config: SpannerConfig, instance: Spanner = defaultInstance): DatabaseClient = {
    instance.getDatabaseClient(
      DatabaseId.of(
        config.getProjectId.get(),
        config.getInstanceId.get(),
        config.getDatabaseId.get()
      ))
  }

  def adminClient(project: String, instance: Spanner = defaultInstance): DatabaseAdminClient =
    SpannerOptions.newBuilder().setProjectId(project).build().getService.getDatabaseAdminClient
}
