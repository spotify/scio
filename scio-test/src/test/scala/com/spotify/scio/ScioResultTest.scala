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

import java.util.concurrent.TimeUnit

import com.spotify.scio.ScioResultTest._
import com.spotify.scio.metrics.{BeamDistribution, MetricValue}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.metrics.MetricResults
import org.joda.time

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.ExecutionContext.Implicits.global

class ScioResultTest extends PipelineSpec {

  "ScioContextResult" should "reflect pipeline state" in {
    val r = runWithContext(_.parallelize(Seq(1, 2, 3)))
    r.isCompleted shouldBe true
    r.state shouldBe State.DONE
  }

  it should "respect waitUntilDone() with cancelJob passed in" in {
    the[PipelineExecutionException] thrownBy {
      mockScioResult.waitUntilDone(cancelJob = true)
    } should have message s"java.lang.Exception: Job cancelled after exceeding timeout value $nanos"

    mockScioResult.state shouldBe State.CANCELLED
  }

  it should "expose Beam metrics" in {
    val r = runWithContext { sc =>
      val c = ScioMetrics.counter("c")
      val d = ScioMetrics.distribution("d")
      val g = ScioMetrics.gauge("g")
      sc.parallelize(Seq(1, 2, 3))
        .map { x =>
          c.inc()
          d.update(x)
          g.set(x)
          x
        }
    }
    val m = r.getMetrics.beamMetrics

    m.counters.map(_.name) shouldBe Iterable("c")
    m.counters.map(_.value) shouldBe Iterable(MetricValue(3L, Some(3L)))

    val dist = BeamDistribution(6L, 3L, 1L, 3L, 2L)
    m.distributions.map(_.name) shouldBe Iterable("d")
    m.distributions.map(_.value) shouldBe Iterable(MetricValue(dist, Some(dist)))

    val gauge = m.gauges.map(_.value)
    m.gauges.map(_.name) shouldBe Iterable("g")
    gauge.forall(g => g.attempted.value >= 1 && g.attempted.value <= 3) shouldBe true
    gauge.forall(g => g.committed.get.value >= 1 && g.committed.get.value <= 3) shouldBe true
  }

  "isTest" should "return true when testing" in {
    val r = runWithContext(_.parallelize(Seq(1, 2, 3)))
    r.isTest shouldBe true
  }

  "isTest" should "return false when not testing" in {
    mockScioResult.isTest shouldBe false
  }
}

object ScioResultTest {

  private val mockPipeline: PipelineResult = new PipelineResult {
    private var state = State.RUNNING
    override def cancel(): State = {
      state = State.CANCELLED
      state
    }
    override def waitUntilFinish(duration: time.Duration): State = null
    override def waitUntilFinish(): State = null
    override def getState: State = state
    override def metrics(): MetricResults = null
  }

  // Give the ScioResult a 10 nanosecond timeout and verify job is cancelled after timeout
  private val nanos = Duration.create(10L, TimeUnit.NANOSECONDS)

  // Mock Scio result takes 100 milliseconds to complete
  private val mockScioResult = new ScioResult(mockPipeline) {
    override def getMetrics: metrics.Metrics = null
    override val finalState: Future[State] = Future {
      Thread.sleep(10.seconds.toMillis)
      State.DONE
    }

    override def getAwaitDuration: Duration = nanos
  }
}
