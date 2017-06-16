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

import org.apache.beam.sdk.metrics.MetricResult

import scala.util.Try

/** This package contains the schema types for metrics collected during a pipeline run. */
package object metrics {

  /**
   * Contains the aggregated value of a metric. See (for example) [[ScioResult.counters]]
   * @param attempted The value aggregated across all attempted steps, including failed steps.
   * @param committed The value aggregated across all completed steps.
   */
  case class MetricValue[T](attempted: T, committed: Option[T])

  private[scio] object MetricValue {
    def apply[T](result: MetricResult[T]): MetricValue[T] =
      MetricValue(result.attempted, Try(result.committed).toOption)
  }

  /**
   * Case class holding metadata and service-level metrics of the job. See
   * [[ScioResult.getMetrics]].
   */
  case class ServiceMetrics(version: String,
                            scalaVersion: String,
                            jobName: String,
                            jobId: String,
                            state: String,
                            cloudMetrics: Iterable[DFServiceMetrics])
  case class DFServiceMetrics(name: DFMetricName, scalar: AnyRef, updateTime: String)
  case class DFMetricName(name: String, origin: String, context: Map[String, String])

}
