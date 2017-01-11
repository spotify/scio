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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.Lists
import com.spotify.scio.metrics.Metrics
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.ScioUtil
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.commons.lang.exception.ExceptionUtils

class ScioContextTest extends PipelineSpec {

  "ScioContext" should "support pipeline" in {
    val pipeline = ScioContext().pipeline
    val p = pipeline.apply(Create.of(Lists.newArrayList(1, 2, 3)))
    PAssert.that(p).containsInAnyOrder(Lists.newArrayList(1, 2, 3))
    pipeline.run()
  }

  it should "have temp location for default runner" in {
    val pipeline = ScioContext().pipeline
    pipeline.getOptions.getTempLocation should not be null
  }

  it should "have temp location for DirectRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectRunner])
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

  it should "support user defined job name" in {
    val jobName = "test-job-1"
    val opts = PipelineOptionsFactory.create().as(classOf[DataflowPipelineOptions])
    opts.setJobName(jobName)
    val pipelineOpts = ScioContext(opts).pipeline.getOptions.as(classOf[DataflowPipelineOptions])
    pipelineOpts.getJobName shouldBe jobName
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on missing temp or staging location for DataflowRunner" in {
    val opts = PipelineOptionsFactory.create().as(classOf[DataflowPipelineOptions])
    opts.setRunner(classOf[DataflowRunner])
    opts.setProject("foobar")
    val sc = ScioContext(opts)
    val e = the [RuntimeException] thrownBy { sc.pipeline }
    ExceptionUtils.getRootCause(e) should have message
      "DataflowRunner requires gcpTempLocation, and it is missing in PipelineOptions."
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "create local output directory on close()" in {
    val output = Files.createTempDirectory("scio-output").toFile
    output.delete()

    val sc = ScioContext()
    sc.parallelize(Seq("a", "b", "c")).saveAsTextFile(output.toString)
    output.exists() shouldBe false

    sc.close()
    output.exists() shouldBe true
    output.delete()
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on close() if output directory exists" in {
    val output = Files.createTempDirectory("scio-output").toFile

    val sc = ScioContext()
    sc.parallelize(Seq("a", "b", "c")).saveAsTextFile(output.toString)

    the [RuntimeException] thrownBy sc.close() should have message
      s"Output directory ${output.toString} already exists"
    output.delete()
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "support save metrics on close for finished pipeline" in {
    val metricsFile = Files.createTempFile("scio-metrics-dump", ".json").toFile
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectRunner])
    opts.as(classOf[ScioOptions]).setMetricsLocation(metricsFile.toString)
    val sc = ScioContext(opts)
    sc.close().waitUntilFinish()  // block non-test runner

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
