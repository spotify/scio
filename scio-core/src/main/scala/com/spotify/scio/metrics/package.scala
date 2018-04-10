/*
 * Copyright 2018 Spotify AB.
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

import org.joda.time.Instant

/** This package contains the schema types for metrics collected during a pipeline run. */
package object metrics {

  /**
   * Contains the aggregated value of a metric. See [[ScioResult.allCounters]],
   * [[ScioResult.allDistributions]] and [[ScioResult.allGauges]].
   * @param attempted The value across all attempts of executing all parts of the pipeline.
   * @param committed The value across all successfully completed parts of the pipeline.
   */
  final case class MetricValue[T](attempted: T, committed: Option[T])

  /**
   * Case class holding metadata and service-level metrics of the job. See
   * [[ScioResult.getMetrics]].
   */
  final case class Metrics(version: String,
                           scalaVersion: String,
                           appName: String,
                           state: String,
                           beamMetrics: BeamMetrics)

  final case class BeamMetrics(counters: Iterable[BeamMetric[Long]],
                               distributions: Iterable[BeamMetric[BeamDistribution]],
                               gauges: Iterable[BeamMetric[BeamGauge]])
  final case class BeamMetric[T](namespace: String, name: String, value: MetricValue[T])
  final case class BeamDistribution(sum: Long, count: Long, min: Long, max: Long, mean: Double)
  final case class BeamGauge(value: Long, timestamp: Instant)

}
