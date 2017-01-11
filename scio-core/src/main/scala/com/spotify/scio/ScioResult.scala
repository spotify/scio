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
import com.spotify.scio.values.Accumulator
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.options.ApplicationNameOptions
import org.apache.beam.sdk.transforms.Aggregator
import org.apache.beam.sdk.util.{IOChannelUtils, MimeTypes}
import org.apache.beam.sdk.{AggregatorValues, PipelineResult}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Represent a Scio pipeline result. */
class ScioResult private[scio] (val internal: PipelineResult,
                                val accumulators: Seq[Accumulator[_]],
                                private val context: ScioContext) {

  private val logger = LoggerFactory.getLogger(classOf[ScioResult])

  private val aggregators: Map[String, Iterable[Aggregator[_, _]]] =
    context.pipeline.getAggregatorSteps.asScala.keys.groupBy(_.getName)

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
      case NonFatal(e) => context.updateFutures(state)
    }
    f
  }

  /** Wait until the pipeline finishes. */
  def waitUntilFinish(duration: Duration = Duration.Inf): Unit =
    Await.ready(finalState, duration)

  /** Whether the pipeline is completed. */
  def isCompleted: Boolean = internal.getState.isTerminal

  /** Pipeline's current state. */
  def state: State = internal.getState

  /** Get the total value of an accumulator. */
  def accumulatorTotalValue[T](acc: Accumulator[T]): T = {
    require(accumulators.contains(acc), "Accumulator not present in the result")
    acc.combineFn(getAggregatorValues(acc).map(_.getTotalValue(acc.combineFn)).asJava)
  }

  /** Get the values of an accumulator at each step it was used. */
  def accumulatorValuesAtSteps[T](acc: Accumulator[T]): Map[String, T] = {
    require(accumulators.contains(acc), "Accumulator not present in the result")
    getAggregatorValues(acc).flatMap(_.getValuesAtSteps.asScala).toMap
  }

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
    val totalValues = accumulators
        .map(acc => AccumulatorValue(acc.name, accumulatorTotalValue(acc)))

    val stepsValues = accumulators.map(acc => AccumulatorStepsValue(acc.name,
      accumulatorValuesAtSteps(acc).map(a => AccumulatorStepValue(a._1, a._2))))

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
          logger.error(s"Failed to fetch dataflow metrics due to $e")
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
      AccumulatorMetrics(totalValues, stepsValues),
      dfMetrics
    )
  }

  private def getAggregatorValues[T](acc: Accumulator[T]): Iterable[AggregatorValues[T]] =
    aggregators.getOrElse(acc.name, Nil)
      .map(a => internal.getAggregatorValues(a.asInstanceOf[Aggregator[_, T]]))

}
