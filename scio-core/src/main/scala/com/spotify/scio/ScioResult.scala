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

import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.runners.{AggregatorValues, AggregatorPipelineExtractor}
import com.google.cloud.dataflow.sdk.{Pipeline, PipelineResult}
import com.google.cloud.dataflow.sdk.transforms.Aggregator
import com.spotify.scio.values.Accumulator

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

  private def getAggregatorValues[T](acc: Accumulator[T]): Iterable[AggregatorValues[T]] =
    aggregators(acc.name).map(a => internal.getAggregatorValues(a.asInstanceOf[Aggregator[_, T]]))

}
