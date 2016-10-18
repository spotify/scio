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

import java.nio.file.Files

import com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.google.cloud.dataflow.sdk.runners.{DataflowPipelineRunner, DirectPipelineRunner}
import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.common.collect.Lists
import com.spotify.scio.MetricSchema.Metrics
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.ScioUtil
import org.apache.commons.lang.exception.ExceptionUtils

import scala.collection.JavaConverters._

class ScioContextTest extends PipelineSpec {

  "ScioContext" should "support applyTransform()" in {
    val sc = ScioContext()
    sc.applyTransform(Create.of((1 to 10).asJava)) should containInAnyOrder (1 to 10)
    sc.close()
  }

  it should "support pipeline" in {
    val pipeline = ScioContext().pipeline
    val p = pipeline.apply(Create.of(Lists.newArrayList(1, 2, 3)))
    DataflowAssert.that(p).containsInAnyOrder(Lists.newArrayList(1, 2, 3))
    pipeline.run()
  }

  it should "have temp location for default runner" in {
    val pipeline = ScioContext().pipeline
    pipeline.getOptions.getTempLocation should not be null
  }

  it should "have temp location for InProcessPipelineRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[InProcessPipelineRunner])
    val pipeline = ScioContext(opts).pipeline
    pipeline.getOptions.getTempLocation should not be null
  }

  it should "have temp location for DirectPipelineRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectPipelineRunner])
    val pipeline = ScioContext(opts).pipeline
    pipeline.getOptions.getTempLocation should not be null
  }

  it should "support user defined temp location" in {
    val expected = "/expected"
    val opts = PipelineOptionsFactory.create()
    opts.setTempLocation(expected)
    val pipeline = ScioContext(opts).pipeline
    pipeline.getOptions.getTempLocation shouldBe expected
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on missing temp or staging location for DataflowPipelineRunner" in {
    val opts = PipelineOptionsFactory.create().as(classOf[DataflowPipelineOptions])
    opts.setRunner(classOf[DataflowPipelineRunner])
    opts.setProject("foobar")
    val sc = ScioContext(opts)
    val e = the [RuntimeException] thrownBy { sc.pipeline }
    ExceptionUtils.getRootCause(e) should have message
      "Missing required value: at least one of tempLocation or stagingLocation must be set."
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "support save metrics on close for finished pipeline" in {
    val metricsFile = Files.createTempFile("scio-metrics-dump", ".json").toFile
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[InProcessPipelineRunner])
    opts.as(classOf[ScioOptions]).setMetricsLocation(metricsFile.toString)
    ScioContext(opts).close()

    val mapper = ScioUtil.getScalaJsonMapper

    val metrics = mapper.readValue(metricsFile, classOf[Metrics])
    metrics.version should be(scioVersion)
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail to close() on closed context" in {
    val sc = ScioContext()
    sc.close()
    the [IllegalArgumentException] thrownBy {
      sc.close()
    } should have message "requirement failed: ScioContext already closed"
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "support containsAccumulator" in {
    val sc = ScioContext()
    val sc1 = ScioContext()
    val max = sc.maxAccumulator[Long]("max")
    val foo = sc1.maxAccumulator[Long]("foo")
    sc.containsAccumulator(max) should be(true)
    sc.containsAccumulator(foo) should be(false)
  }
}
