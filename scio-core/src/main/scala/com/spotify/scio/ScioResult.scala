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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.Accumulator
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.ApplicationNameOptions
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.runners.{AggregatorPipelineExtractor, AggregatorValues}
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.apache.beam.sdk.transforms.Aggregator
import org.apache.beam.sdk.util.{IOChannelUtils, MimeTypes}

import scala.collection.JavaConverters._
import scala.concurrent.Future

/** Represent a Scio pipeline result. */
class ScioResult private[scio] (val internal: PipelineResult,
                                val finalState: Future[State],
                                val accumulators: Seq[Accumulator[_]],
                                private val pipeline: Pipeline) {

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
    acc.combineFn(getAggregatorValues(acc).map(_.getTotalValue(acc.combineFn)).asJava)
  }

  /** Get the values of an accumulator at each step it was used. */
  def accumulatorValuesAtSteps[T](acc: Accumulator[T]): Map[String, T] =
    getAggregatorValues(acc).flatMap(_.getValuesAtSteps.asScala).toMap

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
      Metrics(scioVersion,
        scalaVersion,
        options.as(classOf[ApplicationNameOptions]).getAppName,
        options.as(classOf[DataflowPipelineOptions]).getJobName,
        this.state.toString,
        AccumulatorMetrics(totalValues, stepsValues))
    }
  }

  private def getAggregatorValues[T](acc: Accumulator[T]): Iterable[AggregatorValues[T]] =
    aggregators(acc.name).map(a => internal.getAggregatorValues(a.asInstanceOf[Aggregator[_, T]]))

}

private[scio] object MetricSchema {
  case class Metrics(version: String,
                     scalaVersion: String,
                     jobName: String,
                     jobId: String,
                     state: String,
                     accumulators: AccumulatorMetrics)
  case class AccumulatorMetrics(total: Iterable[AccumulatorValue],
                                steps: Iterable[AccumulatorStepsValue])
  case class AccumulatorValue(name: String, value: Any)
  case class AccumulatorStepValue(name: String, value: Any)
  case class AccumulatorStepsValue(name: String, steps: Iterable[AccumulatorStepValue])
}
