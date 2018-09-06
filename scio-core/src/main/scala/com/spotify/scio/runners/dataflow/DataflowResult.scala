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

import java.util.Collections

import com.google.api.services.dataflow.Dataflow
import com.google.api.services.dataflow.model.{Job, JobMetrics}
import com.spotify.scio.metrics.Metrics
import com.spotify.scio.{RunnerResult, ScioResult}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.runners.dataflow.{DataflowClient, DataflowPipelineJob}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.runners.AppliedPTransform
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PInput, POutput}
import org.apache.beam.sdk.{Pipeline, PipelineResult}

import scala.collection.JavaConverters._
import scala.concurrent.Future

/** Represent a Dataflow runner specific result. */
class DataflowResult(val internal: DataflowPipelineJob) extends RunnerResult {

  def this(internal: PipelineResult) = this(internal.asInstanceOf[DataflowPipelineJob])

  private val client = DataflowResult.getOptions(internal.getProjectId).getDataflowClient

  /** Get Dataflow [[com.google.api.services.dataflow.model.Job Job]]. */
  def getJob: Job = DataflowResult.getJob(
    client, internal.getProjectId, internal.getRegion, internal.getJobId)

  /** Get Dataflow [[com.google.api.services.dataflow.model.JobMetrics JobMetrics]]. */
  def getJobMetrics: JobMetrics = DataflowResult.getJobMetrics(
    client, internal.getProjectId, internal.getRegion, internal.getJobId)

  /** Get a generic [[ScioResult]]. */
  override def asScioResult: ScioResult = new DataflowScioResult(internal)

  private class DataflowScioResult(internal: PipelineResult) extends ScioResult(internal) {
    override val finalState: Future[PipelineResult.State] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      Future {
        internal.waitUntilFinish()
        this.state
      }
    }

    override def getMetrics: Metrics = {
      val options = getJob.getEnvironment.getSdkPipelineOptions.get("options")
        .asInstanceOf[java.util.Map[String, AnyRef]]
      Metrics(
        options.get("scioVersion").toString,
        options.get("scalaVersion").toString,
        options.get("appName").toString,
        internal.getState.toString,
        getBeamMetrics)
    }
  }

}

/** Companion object for [[DataflowResult]]. */
object DataflowResult {
  /** Create a new [[DataflowResult]] instance. */
  def apply(projectId: String, jobId: String): DataflowResult = {
    val options = getOptions(projectId)

    val job = getJob(options.getDataflowClient, options.getProject, options.getRegion, jobId)
    // DataflowPipelineJob require a mapping of human-readable transform names via
    // AppliedPTransform#getUserName, e.g. flatMap@MyJob.scala:12, to Dataflow service generated
    // transform names, e.g. s12
    val transformStepNames: Map[AppliedPTransform[_, _, _], String] =
      job.getPipelineDescription.getExecutionPipelineStage.asScala
        .flatMap { s =>
          if (s.getComponentTransform != null) {
            s.getComponentTransform.asScala.map { t =>
              newAppliedPTransform(t.getUserName) -> t.getName
            }
          } else {
            Nil
          }
        }(scala.collection.breakOut)

    val client = DataflowClient.create(options)
    val internal = new DataflowPipelineJob(client, jobId, options, transformStepNames.asJava)
    new DataflowResult(internal)
  }

  private def getOptions(projectId: String): DataflowPipelineOptions = {
    val options = PipelineOptionsFactory.create().as(classOf[DataflowPipelineOptions])
    options.setProject(projectId)
    options
  }

  private def getJob(dataflow: Dataflow, projectId: String, location: String, jobId: String): Job =
    dataflow.projects().locations().jobs().get(projectId, location, jobId).setView("JOB_VIEW_ALL")
      .execute()

  private def getJobMetrics(dataflow: Dataflow, projectId: String, location: String,
                            jobId: String): JobMetrics =
    dataflow.projects().locations().jobs().getMetrics(projectId, location, jobId).execute()

  // wiring to reconstruct AppliedPTransform for name mapping

  private def newAppliedPTransform(fullName: String)
  : AppliedPTransform[PInput, POutput, EmptyPTransform] = AppliedPTransform.of(
    fullName, Collections.emptyMap(), Collections.emptyMap(),
    new EmptyPTransform, new EmptyPipeline)

  private class EmptyPTransform extends PTransform[PInput, POutput] {
    override def expand(input: PInput): POutput = ???
  }

  private class EmptyPipeline extends Pipeline(null)

}
