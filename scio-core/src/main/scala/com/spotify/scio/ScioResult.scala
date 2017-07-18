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

import com.spotify.scio.metrics._
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.util.ScioUtil
import com.twitter.algebird.Semigroup
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.options.ApplicationNameOptions
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.{metrics => bm}
import org.apache.beam.sdk.util.MimeTypes
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Represent a Scio pipeline result. */
class ScioResult private[scio] (val internal: PipelineResult, val context: ScioContext) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * `Future` for pipeline's final state. The `Future` will be completed once the pipeline
   * completes successfully.
   */
  val finalState: Future[State] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val f = Future {
      val state = internal.waitUntilFinish()
      context.updateFutures(state)
      val metricsLocation = context.optionsAs[ScioOptions].getMetricsLocation
      if (metricsLocation != null) {
        saveMetrics(metricsLocation)
      }
      this.state
    }
    f.onComplete {
      case Success(_) => Unit
      case Failure(NonFatal(_)) => context.updateFutures(state)
    }
    f
  }

  /** Wait until the pipeline finishes. */
  def waitUntilFinish(duration: Duration = Duration.Inf): ScioResult = {
    Await.ready(finalState, duration)
    this
  }

  /**
   * Wait until the pipeline finishes with the State `DONE` (as opposed to `CANCELLED` or
   * `FAILED`). Throw exception otherwise.
   */
  def waitUntilDone(duration: Duration = Duration.Inf): ScioResult = {
    waitUntilFinish(duration)
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

  /** Get metrics of the finished pipeline. */
  // scalastyle:off method.length
  def getMetrics: Metrics = {
    require(isCompleted, "Pipeline has to be finished to get metrics.")

    val (jobId, dfMetrics) = if (ScioUtil.isLocalRunner(this.context.options)) {
      // to be safe let's use app name at a cost of duplicate for local runner
      // there are no dataflow service metrics on local runner
      (context.optionsAs[ApplicationNameOptions].getAppName, Nil)
    } else {
      val jobId = internal.asInstanceOf[DataflowPipelineJob].getJobId
      // given that this depends on internals of dataflow service - handle failure gracefully
      // if there is an error - no dataflow service metrics will be saved
      val dfMetrics = Try {
        ScioUtil
          .getDataflowServiceMetrics(context.optionsAs[DataflowPipelineOptions], jobId)
          .getMetrics.asScala
          .map(e => {
            val name = DFMetricName(e.getName.getName,
              e.getName.getOrigin,
              Option(e.getName.getContext)
                    .getOrElse(Map.empty[String, String].asJava).asScala.toMap)
            DFServiceMetrics(name, e.getScalar, e.getUpdateTime)
          })
      } match {
        case Success(x) => x
        case Failure(e) =>
          logger.error(s"Failed to fetch Dataflow metrics", e)
          Nil
      }
      (jobId, dfMetrics)
    }

    def mkDist(d: bm.DistributionResult): BeamDistribution =
      BeamDistribution(d.sum(), d.count(), d.min(), d.max(), d.mean())
    def mkGauge(g: bm.GaugeResult): BeamGauge = BeamGauge(g.value(), g.timestamp())

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
    Metrics(scioVersion,
      scalaVersion,
      context.optionsAs[ApplicationNameOptions].getAppName,
      jobId,
      state.toString,
      BeamMetrics(beamCounters, beamDistributions, beamGauges),
      dfMetrics)
  }
  // scalastyle:on method.length

  /** Retrieve aggregated value of a single counter from the pipeline. */
  def counter(c: bm.Counter): MetricValue[Long] = allCounters(c.getName)

  /** Retrieve aggregated value of a single distribution from the pipeline. */
  def distribution(d: bm.Distribution): MetricValue[bm.DistributionResult] =
    allDistributions(d.getName)

  /** Retrieve latest value of a single gauge from the pipeline. */
  def gauge(g: bm.Gauge): MetricValue[bm.GaugeResult] = allGauges(g.getName)

  /** Retrieve per step values of a single counter from the pipeline. */
  def counterAtSteps(c: bm.Counter): Map[String, MetricValue[Long]] = allCountersAtSteps(c.getName)

  /** Retrieve per step values of a single distribution from the pipeline. */
  def distributionAtSteps(d: bm.Distribution): Map[String, MetricValue[bm.DistributionResult]] =
    allDistributionsAtSteps(d.getName)

  /** Retrieve per step values of a single gauge from the pipeline. */
  def gaugeAtSteps(g: bm.Gauge): Map[String, MetricValue[bm.GaugeResult]] =
    allGaugesAtSteps(g.getName)

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
