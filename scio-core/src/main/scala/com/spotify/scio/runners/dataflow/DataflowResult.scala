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

package com.spotify.scio.runners.dataflow

import com.google.api.services.dataflow.model.{Job, JobMetrics}
import com.spotify.scio.RunnerResult
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.runners.dataflow.{DataflowClient, DataflowPipelineJob}
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.options.PipelineOptionsFactory

/** Represent a Dataflow runner specific result. */
class DataflowResult(val internal: DataflowPipelineJob) extends RunnerResult {
  def this(result: PipelineResult) = this(result.asInstanceOf[DataflowPipelineJob])

  private val client = DataflowClient.create(DataflowResult.getOptions(internal.getProjectId))

  /**  Get Dataflow [[Job]]. */
  def getJob: Job = client.getJob(internal.getJobId)

  /**  Get Dataflow [[JobMetrics]]. */
  def getJobMetrics: JobMetrics = client.getJobMetrics(internal.getJobId)
}

/** Companion object for [[DataflowResult]]. */
object DataflowResult {
  /** Create a new [[DataflowResult]] instance. */
  def apply(projectId: String, jobId: String): DataflowResult = {
    val options = getOptions(projectId)
    val client = DataflowClient.create(options)
    val job = new DataflowPipelineJob(client, jobId, options, null)
    new DataflowResult(job)
  }

  private def getOptions(projectId: String): DataflowPipelineOptions = {
    val options = PipelineOptionsFactory.create().as(classOf[DataflowPipelineOptions])
    options.setProject(projectId)
    options
  }
}
