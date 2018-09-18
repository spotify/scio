/*
 * Copyright 2017 Spotify AB.
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

import org.apache.beam.sdk.metrics.{Counter, Distribution, Gauge, Metrics}
import scala.reflect.ClassTag

/**
 * Utility object for creating metrics. The main types available are
 * [[org.apache.beam.sdk.metrics.Counter]], [[org.apache.beam.sdk.metrics.Distribution]] and
 * [[org.apache.beam.sdk.metrics.Gauge]].
 */
object ScioMetrics {

  private def namespace[T: ClassTag]: String = {
    val cls = implicitly[ClassTag[T]].runtimeClass
    val ns: Class[_] =
      if (classOf[Nothing] isAssignableFrom cls) this.getClass else cls
    ns.getCanonicalName.replaceAll("\\$$", "")
  }

  /** Create a new [[org.apache.beam.sdk.metrics.Counter Counter]] metric. */
  def counter(namespace: String, name: String): Counter =
    Metrics.counter(namespace, name)

  /**
   * Create a new [[org.apache.beam.sdk.metrics.Counter Counter]] metric using `T` as namespace.
   * Default is "com.spotify.scio.ScioMetrics" if `T` is not specified.
   */
  def counter[T: ClassTag](name: String): Counter = counter(namespace[T], name)

  /** Create a new [[org.apache.beam.sdk.metrics.Distribution Distribution]] metric. */
  def distribution(namespace: String, name: String): Distribution =
    Metrics.distribution(namespace, name)

  /**
   * Create a new [[org.apache.beam.sdk.metrics.Distribution Distribution]] metric using `T` as
   * namespace. Default is "com.spotify.scio.ScioMetrics" if `T` is not specified.
   */
  def distribution[T: ClassTag](name: String): Distribution =
    distribution(namespace[T], name)

  /** Create a new [[org.apache.beam.sdk.metrics.Gauge Gauge]] metric. */
  def gauge(namespace: String, name: String): Gauge =
    Metrics.gauge(namespace, name)

  /**
   * Create a new [[org.apache.beam.sdk.metrics.Gauge Gauge]] metric using `T` as namespace.
   * Default is "com.spotify.scio.ScioMetrics" if `T` is not specified.
   */
  def gauge[T: ClassTag](name: String): Gauge = gauge(namespace[T], name)

}
