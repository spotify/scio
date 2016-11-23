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

/**
  * This package contains the schema types for metrics collected during a pipeline run.
  *
  * See [[ScioResult.getMetrics]]
  */
package object metrics {
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
