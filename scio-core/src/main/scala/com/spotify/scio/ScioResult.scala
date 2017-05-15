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

package com.spotify.scio


import java.nio.ByteBuffer

import com.spotify.scio.metrics._
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.util.ScioUtil
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.options.ApplicationNameOptions
import org.apache.beam.sdk.util.{IOChannelUtils, MimeTypes}
import org.apache.beam.sdk.PipelineResult
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Represent a Scio pipeline result. */
class ScioResult private[scio] (val internal: PipelineResult,
                                val context: ScioContext) {

  private val logger = LoggerFactory.getLogger(this.getClass)


  /**
   * `Future` for pipeline's final state. The `Future` will be completed once the pipeline
   * completes successfully.
   */
  val finalState: Future[State] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val f = Future {
      val state = internal.waitUntilFinish()
      context.updateFutures(state)
      val metricsLocation = context.optionsAs[ScioOptions].getMetricsLocation
      if (metricsLocation != null) {
        saveMetrics(metricsLocation)
      }
      this.state
    }
    f.onFailure {
      case NonFatal(_) => context.updateFutures(state)
    }
    f
  }

  /** Wait until the pipeline finishes. */
  def waitUntilFinish(duration: Duration = Duration.Inf): ScioResult = {
    Await.ready(finalState, duration)
    this
  }

  /**
   * Wait until the pipeline finishes with the State `DONE` (as opposed to `CANCELLED` or
   * `FAILED`). Throw exception otherwise.
   */
  def waitUntilDone(duration: Duration = Duration.Inf): ScioResult = {
    waitUntilFinish(duration)
    if (!this.state.equals(State.DONE)) {
      throw new PipelineExecutionException(new Exception(s"Job finished with state ${this.state}"))
    }
    this
  }

  /** Whether the pipeline is completed. */
  def isCompleted: Boolean = internal.getState.isTerminal

  /** Pipeline's current state. */
  def state: State = Try(internal.getState).getOrElse(State.UNKNOWN)

  /** Save metrics of the finished pipeline to a file. */
  def saveMetrics(filename: String): Unit = {
    require(isCompleted, "Pipeline has to be finished to save metrics.")
    val mapper = ScioUtil.getScalaJsonMapper
    val out = IOChannelUtils.create(filename, MimeTypes.TEXT)
    try {
      out.write(ByteBuffer.wrap(mapper.writeValueAsBytes(getMetrics)))
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  /** Get metrics of the finished pipeline. */
  def getMetrics: Metrics = {
    require(isCompleted, "Pipeline has to be finished to get metrics.")

    val (jobId, dfMetrics) = if (ScioUtil.isLocalRunner(this.context.options)) {
      // to be safe let's use app name at a cost of duplicate for local runner
      // there are no dataflow service metrics on local runner
      (context.optionsAs[ApplicationNameOptions].getAppName, Nil)
    } else {
      val jobId = internal.asInstanceOf[DataflowPipelineJob].getJobId
      // given that this depends on internals of dataflow service - handle failure gracefully
      // if there is an error - no dataflow service metrics will be saved
      val dfMetrics = Try {
        ScioUtil
          .getDataflowServiceMetrics(context.optionsAs[DataflowPipelineOptions], jobId)
          .getMetrics.asScala
          .map(e => {
            val name = DFMetricName(e.getName.getName,
              e.getName.getOrigin,
              Option(e.getName.getContext)
                    .getOrElse(Map.empty[String, String].asJava).asScala.toMap)
            DFServiceMetrics(name, e.getScalar, e.getUpdateTime)
          })
      } match {
        case Success(x) => x
        case Failure(e) => {
          logger.error(s"Failed to fetch Dataflow metrics", e)
          Nil
        }
      }
      (jobId, dfMetrics)
    }

    Metrics(scioVersion,
      scalaVersion,
      context.optionsAs[ApplicationNameOptions].getAppName,
      jobId,
      this.state.toString,
      dfMetrics
    )
  }

}
