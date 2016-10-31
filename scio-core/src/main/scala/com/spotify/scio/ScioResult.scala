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

import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.options.{ApplicationNameOptions, DataflowPipelineOptions}
import com.google.cloud.dataflow.sdk.runners.{AggregatorPipelineExtractor,
                                              AggregatorValues,
                                              DataflowPipelineJob}
import com.google.cloud.dataflow.sdk.transforms.Aggregator
import com.google.cloud.dataflow.sdk.util._
import com.google.cloud.dataflow.sdk.{Pipeline, PipelineResult}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.Accumulator
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** Represent a Scio pipeline result. */
class ScioResult private[scio] (val internal: PipelineResult,
                                val finalState: Future[State],
                                val accumulators: Seq[Accumulator[_]],
                                private val pipeline: Pipeline) {

  private val logger = LoggerFactory.getLogger(classOf[ScioResult])

  private val aggregators: Map[String, Iterable[Aggregator[_, _]]] =
    new AggregatorPipelineExtractor(pipeline)
      .getAggregatorSteps
      .asScala
      .keys
      .groupBy(_.getName)

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

  // scalastyle:off method.length
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

    def getMetrics: MetricSchema.Metrics = {
      import MetricSchema._

      val totalValues = accumulators
        .map(acc => AccumulatorValue(acc.name, accumulatorTotalValue(acc)))

      val stepsValues = accumulators.map(acc => AccumulatorStepsValue(acc.name,
        accumulatorValuesAtSteps(acc).map(a => AccumulatorStepValue(a._1, a._2))))

      val options = this.pipeline.getOptions

      val (jobId, dfMetrics) = if (ScioUtil.isLocalRunner(options)) {
        // to be safe let's use app name at a cost of duplicate for local runner
        // there are no dataflow service metrics on local runner
        (options.as(classOf[ApplicationNameOptions]).getAppName, Nil)
      } else {
        val jobId = internal.asInstanceOf[DataflowPipelineJob].getJobId
        // given that this depends on internals of dataflow service - handle failure gracefully
        // if there is an error - no dataflow service metrics will be saved
        val dfMetrics = Try {
          ScioUtil
            .getDataflowServiceMetrics(options.as(classOf[DataflowPipelineOptions]), jobId)
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
        options.as(classOf[ApplicationNameOptions]).getAppName,
        jobId,
        this.state.toString,
        AccumulatorMetrics(totalValues, stepsValues),
        dfMetrics
        )
    }
  }
  // scalastyle:on method.length

  private def getAggregatorValues[T](acc: Accumulator[T]): Iterable[AggregatorValues[T]] =
    aggregators.getOrElse(acc.name, Nil)
      .map(a => internal.getAggregatorValues(a.asInstanceOf[Aggregator[_, T]]))

}

private[scio] object MetricSchema {
  case class Metrics(version: String,
                     scalaVersion: String,
                     jobName: String,
                     jobId: String,
                     state: String,
                     accumulators: AccumulatorMetrics,
                     cloudMetrics: Iterable[DFServiceMetrics])
  case class DFServiceMetrics(name: DFMetricName,
                              scalar: AnyRef,
                              updateTime: String)
  case class DFMetricName(name: String, origin: String, context: Map[String, String])
  case class AccumulatorMetrics(total: Iterable[AccumulatorValue],
                                steps: Iterable[AccumulatorStepsValue])
  case class AccumulatorValue(name: String, value: Any)
  case class AccumulatorStepValue(name: String, value: Any)
  case class AccumulatorStepsValue(name: String, steps: Iterable[AccumulatorStepValue])
}
