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
import java.util.concurrent.TimeoutException

import com.spotify.scio.metrics._
import com.spotify.scio.util.ScioUtil
import com.twitter.algebird.Semigroup
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.metrics.{DistributionResult, GaugeResult}
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.{PipelineResult, metrics => bm}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.Try

/** Represent a Beam runner specific result. */
trait RunnerResult {
  /** Get a generic [[ScioResult]]. */
  def asScioResult: ScioResult
}

/** Represent a Scio pipeline result. */
abstract class ScioResult private[scio] (val internal: PipelineResult) {

  /** Get a Beam runner specific result. */
  def as[T <: RunnerResult : ClassTag]: T = {
    val cls = ScioUtil.classOf[T]
    try {
      cls.getConstructor(classOf[PipelineResult]).newInstance(internal)
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Failed to construct runner specific result $cls", e)
    }
  }

  /**
   * `Future` for pipeline's final state. The `Future` will be completed once the pipeline
   * completes successfully.
   */
  val finalState: Future[State]

  /** Get metrics of the finished pipeline. */
  def getMetrics: Metrics

  /** Get the timeout period of the Scio job. Default to `Duration.Inf`. */
  def getAwaitDuration: Duration = Duration.Inf

  /** Wait until the pipeline finishes. If timeout duration is exceeded and `cancelJob` is set,
    * cancel the internal [[PipelineResult]]. */
  def waitUntilFinish(duration: Duration = getAwaitDuration, cancelJob: Boolean = true):
  ScioResult = {
    try {
      Await.ready(finalState, duration)
    } catch {
      case e: TimeoutException =>
        if (cancelJob) {
          internal.cancel()
          throw new PipelineExecutionException(
            new Exception(s"Job cancelled after exceeding timeout value $duration"))
        }
    }
    this
  }

  /**
   * Wait until the pipeline finishes with the State `DONE` (as opposed to `CANCELLED` or
   * `FAILED`). Throw exception otherwise.
   */
  def waitUntilDone(duration: Duration = getAwaitDuration, cancelJob: Boolean = true):
  ScioResult = {
    waitUntilFinish(duration, cancelJob)

    if (!this.state.equals(State.DONE)) {
      throw new PipelineExecutionException(new Exception(s"Job finished with state ${this.state}"))
    }
    this
  }

  /** Whether the pipeline is completed. */
  def isCompleted: Boolean = internal.getState.isTerminal

  /** Pipeline's current state. */
  def state: State = Try(internal.getState).getOrElse(State.UNKNOWN)

  /** Save metrics of the finished pipeline to a file. */
  def saveMetrics(filename: String): Unit = {
    require(isCompleted, "Pipeline has to be finished to save metrics.")
    val mapper = ScioUtil.getScalaJsonMapper
    val resourceId = FileSystems.matchNewResource(filename, false)
    val out = FileSystems.create(resourceId, MimeTypes.TEXT)
    try {
      out.write(ByteBuffer.wrap(mapper.writeValueAsBytes(getMetrics)))
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  protected def getBeamMetrics: BeamMetrics = {
    require(isCompleted, "Pipeline has to be finished to get metrics.")

    def mkDist(d: bm.DistributionResult): BeamDistribution = {
      val dist = Option(d).getOrElse(DistributionResult.IDENTITY_ELEMENT)
      BeamDistribution(dist.sum(), dist.count(), dist.min(), dist.max(), dist.mean())
    }
    def mkGauge(g: bm.GaugeResult): BeamGauge = {
      val gauge = Option(g).getOrElse(GaugeResult.empty())
      BeamGauge(gauge.value(), gauge.timestamp())
    }
    val beamCounters = allCounters.map { case (k, v) =>
      BeamMetric(k.namespace(), k.name(), v)
    }
    val beamDistributions = allDistributions.map { case (k, v) =>
      BeamMetric(k.namespace(), k.name(), MetricValue(mkDist(v.attempted), v.committed.map(mkDist)))
    }
    val beamGauges = allGauges.map { case (k, v) =>
      BeamMetric(k.namespace(), k.name(),
        MetricValue(mkGauge(v.attempted), v.committed.map(mkGauge)))
    }
    BeamMetrics(beamCounters, beamDistributions, beamGauges)
  }

  /** Retrieve aggregated value of a single counter from the pipeline. */
  def counter(c: bm.Counter): MetricValue[Long] = getMetric(allCounters, c.getName)

  /** Retrieve aggregated value of a single distribution from the pipeline. */
  def distribution(d: bm.Distribution): MetricValue[bm.DistributionResult] =
    getMetric(allDistributions, d.getName)

  /** Retrieve latest value of a single gauge from the pipeline. */
  def gauge(g: bm.Gauge): MetricValue[bm.GaugeResult] = getMetric(allGauges, g.getName)

  /** Retrieve per step values of a single counter from the pipeline. */
  def counterAtSteps(c: bm.Counter): Map[String, MetricValue[Long]] =
    getMetric(allCountersAtSteps, c.getName)

  /** Retrieve per step values of a single distribution from the pipeline. */
  def distributionAtSteps(d: bm.Distribution): Map[String, MetricValue[bm.DistributionResult]] =
    getMetric(allDistributionsAtSteps, d.getName)

  /** Retrieve per step values of a single gauge from the pipeline. */
  def gaugeAtSteps(g: bm.Gauge): Map[String, MetricValue[bm.GaugeResult]] =
    getMetric(allGaugesAtSteps, g.getName)

  private def getMetric[V](m: Map[bm.MetricName, V], k: bm.MetricName): V = m.get(k) match {
    case Some(value) => value
    case None =>
      val e = new NoSuchElementException(
        s"metric not found: $k, the metric might not have been accessed inside the pipeline")
      throw e
  }

  /** Retrieve aggregated values of all counters from the pipeline. */
  lazy val allCounters: Map[bm.MetricName, MetricValue[Long]] =
    allCountersAtSteps.mapValues(reduceMetricValues[Long])

  /** Retrieve aggregated values of all distributions from the pipeline. */
  lazy val allDistributions: Map[bm.MetricName, MetricValue[bm.DistributionResult]] = {
    implicit val distributionResultSg = Semigroup.from[bm.DistributionResult] { (x, y) =>
      bm.DistributionResult.create(
        x.sum() + y.sum(), x.count() + y.count(),
        math.min(x.min(), y.min()), math.max(x.max(), y.max()))
    }
    allDistributionsAtSteps.mapValues(reduceMetricValues[bm.DistributionResult])
  }

  /** Retrieve latest values of all gauges from the pipeline. */
  lazy val allGauges: Map[bm.MetricName, MetricValue[bm.GaugeResult]] = {
    implicit val gaugeResultSg = Semigroup.from[bm.GaugeResult] { (x, y) =>
      // sum by taking the latest
      if (x.timestamp() isAfter y.timestamp()) x else y
    }
    allGaugesAtSteps.mapValues(reduceMetricValues[bm.GaugeResult])
  }

  /** Retrieve per step values of all counters from the pipeline. */
  lazy val allCountersAtSteps: Map[bm.MetricName, Map[String, MetricValue[Long]]] =
    metricsAtSteps(
      internalMetrics.counters().asScala.asInstanceOf[Iterable[bm.MetricResult[Long]]])

  /** Retrieve per step values of all distributions from the pipeline. */
  lazy val allDistributionsAtSteps
  : Map[bm.MetricName, Map[String, MetricValue[bm.DistributionResult]]] =
    metricsAtSteps(internalMetrics.distributions().asScala)

  /** Retrieve aggregated values of all gauges from the pipeline. */
  lazy val allGaugesAtSteps: Map[bm.MetricName, Map[String, MetricValue[bm.GaugeResult]]] =
    metricsAtSteps(internalMetrics.gauges().asScala)

  private lazy val internalMetrics = internal.metrics.queryMetrics(
    bm.MetricsFilter.builder().build())

  private def metricsAtSteps[T](results: Iterable[bm.MetricResult[T]])
  : Map[bm.MetricName, Map[String, MetricValue[T]]] =
    results
      .groupBy(_.name())
      .mapValues { xs =>
        val m: Map[String, MetricValue[T]] = xs.map { r =>
          r.step() -> MetricValue(r.attempted(), Try(r.committed()).toOption)
        } (scala.collection.breakOut)
        m
      }

  private def reduceMetricValues[T: Semigroup](xs: Map[String, MetricValue[T]]) = {
    val sg = Semigroup.from[MetricValue[T]] { (x, y) =>
      val sg = implicitly[Semigroup[T]]
      val sgO = implicitly[Semigroup[Option[T]]]
      MetricValue(sg.plus(x.attempted, y.attempted), sgO.plus(x.committed, y.committed))
    }
    xs.values.reduce(sg.plus)
  }

}
