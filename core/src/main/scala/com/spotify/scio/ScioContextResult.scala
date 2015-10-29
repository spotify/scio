package com.spotify.scio

import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.runners.{AggregatorValues, AggregatorPipelineExtractor}
import com.google.cloud.dataflow.sdk.{Pipeline, PipelineResult}
import com.google.cloud.dataflow.sdk.transforms.Aggregator
import com.spotify.scio.values.Accumulator

import scala.collection.JavaConverters._

/** Represent a ScioContext result. */
class ScioContextResult private[scio] (val internal: PipelineResult, pipeline: Pipeline) {

  private val aggregators: Map[String, Iterable[Aggregator[_, _]]] =
    new AggregatorPipelineExtractor(pipeline)
      .getAggregatorSteps
      .asScala
      .keys
      .groupBy(_.getName)

  /** Whether the context is completed. */
  def isCompleted: Boolean = internal.getState.isTerminal

  /** Pipeline result state. */
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
