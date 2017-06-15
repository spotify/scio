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

import org.apache.beam.sdk.metrics.{Counter, Distribution, Gauge, MetricResult, Metrics => BMetrics}

import scala.util.Try

/**
 * This package contains the schema types for metrics collected during a pipeline run.
 *
 * See [[ScioResult.getMetrics]].
 */
package object metrics {

  /** Utility object for creating Metrics. The main types available are
   * [[org.apache.beam.sdk.metrics.Counter]], [[org.apache.beam.sdk.metrics.Distribution]] and
   * [[org.apache.beam.sdk.metrics.Gauge]].
   */
  object Metrics {
    private[scio] val namespace = "scio"
    def counter(name: String): Counter = BMetrics.counter(namespace, name)
    def distribution(name: String): Distribution = BMetrics.distribution(namespace, name)
    def gauge(name: String): Gauge = BMetrics.gauge(namespace, name)
  }

  /** Contains the aggregated value of a metric.
   *
   * @param attempted The value aggregated across all attempted steps, including failed steps.
   * @param committed The value aggregated across all completed steps.
   */
  case class MetricValue[T](attempted: T, committed: Option[T])

  private[scio] object MetricValue {
    def apply[T](result: MetricResult[T]): MetricValue[T] =
      new MetricValue(result.attempted, Try(result.committed).toOption)
  }

  /** Case class holding metadata and service-level metrics of the job. */
  case class Metrics(version: String,
                     scalaVersion: String,
                     jobName: String,
                     jobId: String,
                     state: String,
                     cloudMetrics: Iterable[DFServiceMetrics])
  case class DFServiceMetrics(name: DFMetricName, scalar: AnyRef, updateTime: String)
  case class DFMetricName(name: String, origin: String, context: Map[String, String])
}
