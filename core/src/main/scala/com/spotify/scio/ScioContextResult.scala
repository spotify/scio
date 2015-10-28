package com.spotify.scio

import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.runners.{AggregatorValues, AggregatorPipelineExtractor}
import com.google.cloud.dataflow.sdk.{Pipeline, PipelineResult}
import com.google.cloud.dataflow.sdk.transforms.Aggregator
import com.spotify.scio.values.Accumulator

import scala.collection.JavaConverters._

/** Represent a ScioContext result. */
class ScioContextResult private[scio] (val internal: PipelineResult, pipeline: Pipeline) {

  private val aggregators: Map[String, Aggregator[_, _]] =
    new AggregatorPipelineExtractor(pipeline)
      .getAggregatorSteps
      .asScala
      .map (kv =>  kv._1.getName -> kv._1)
      .toMap

  /** Whether the context is completed. */
  def isCompleted: Boolean = internal.getState.isTerminal

  /** Pipeline result state. */
  def state: State = internal.getState

  /** Get the value of an accumulator. */
  def accumulatorValue[T](acc: Accumulator[T]): T =
    internal.getAggregatorValues(aggregators(acc.name).asInstanceOf[Aggregator[_, T]]).getTotalValue(acc.combineFn)

}
