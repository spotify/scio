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

import java.io.PrintWriter
import java.nio.file.Files

import com.google.common.collect.Lists
import com.spotify.scio.metrics.Metrics
import com.spotify.scio.nio.TextIO
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing.{PipelineSpec, TestValidationOptions}
import com.spotify.scio.util.ScioUtil
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create

import scala.concurrent.duration.Duration

class ScioContextTest extends PipelineSpec {

  "ScioContext" should "support pipeline" in {
    val pipeline = ScioContext().pipeline
    val p = pipeline.apply(Create.of(Lists.newArrayList(1, 2, 3)))
    PAssert.that(p).containsInAnyOrder(Lists.newArrayList(1, 2, 3))
    pipeline.run()
  }

  it should "have temp location for default runner" in {
    val opts = ScioContext().options
    opts.getTempLocation should not be null
  }

  it should "have temp location for DirectRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectRunner])
    ScioContext(opts).options.getTempLocation should not be null
  }

  it should "support user defined temp location" in {
    val expected = "/expected"
    val opts = PipelineOptionsFactory.create()
    opts.setTempLocation(expected)
    ScioContext(opts).options.getTempLocation shouldBe expected
  }

  it should "support user defined job name via options" in {
    val jobName = "test-job-1"
    val opts = PipelineOptionsFactory.create()
    opts.setJobName(jobName)
    val pipelineOpts = ScioContext(opts).options
    pipelineOpts.getJobName shouldBe jobName
  }

  it should "support user defined job name via context" in {
    val jobName = "test-job-1"
    val opts = PipelineOptionsFactory.create()
    val sc = ScioContext(opts)
    sc.setJobName(jobName)
    val pipelineOpts = ScioContext(opts).options
    pipelineOpts.getJobName shouldBe jobName
  }

  it should "support user defined job name via options then context" in {
    val jobName1 = "test-job-1"
    val jobName2 = "test-job-2"
    val opts = PipelineOptionsFactory.create()
    opts.setJobName(jobName1)
    val sc = ScioContext(opts)
    sc.setJobName(jobName2)
    val pipelineOpts = ScioContext(opts).options
    pipelineOpts.getJobName shouldBe jobName2
  }

  it should "create local output directory on close()" in {
    val output = Files.createTempDirectory("scio-output-").toFile
    output.delete()

    val sc = ScioContext()
    sc.parallelize(Seq("a", "b", "c")).saveAsTextFile(output.toString)
    output.exists() shouldBe false

    sc.close()
    output.exists() shouldBe true
    output.delete()
  }

  it should "[nio] create local output directory on close()" in {
    val output = Files.createTempDirectory("scio-output-").toFile
    output.delete()

    val sc = ScioContext()
    val textIO = TextIO(output.getAbsolutePath)
    sc.parallelize(Seq("a", "b", "c")).write(textIO)(TextIO.WriteParam())
    output.exists() shouldBe false

    sc.close()
    output.exists() shouldBe true
    output.delete()
  }

  it should "support save metrics on close for finished pipeline" in {
    val metricsFile = Files.createTempFile("scio-metrics-dump-", ".json").toFile
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectRunner])
    opts.as(classOf[ScioOptions]).setMetricsLocation(metricsFile.toString)
    val sc = ScioContext(opts)
    sc.close().waitUntilFinish()  // block non-test runner

    val mapper = ScioUtil.getScalaJsonMapper

    val metrics = mapper.readValue(metricsFile, classOf[Metrics])
    metrics.version shouldBe BuildInfo.version
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

  it should "support options from optionsFile" in {
    val optionsFile = Files.createTempFile("scio-options-", ".txt").toFile
    val pw = new PrintWriter(optionsFile)
    try {
      pw.append("--foo=bar")
      pw.flush()
    } finally {
      pw.close()
    }
    val (_, arg) = ScioContext.parseArguments[PipelineOptions](
      Array(s"--optionsFile=${optionsFile.getAbsolutePath}"))
    arg("foo") shouldBe "bar"
  }

  it should "invalidate options where required arguments are missing" in {
    assertThrows[IllegalArgumentException] {
      ScioContext.parseArguments[TestValidationOptions](Array("--foo=bar"), true)
    }
  }

  it should "parse valid, invalid, and missing blockFor argument passed from command line" in {
    val (validOpts, _) = ScioContext.parseArguments[PipelineOptions](Array(s"--blockFor=1h"))
    ScioContext.apply(validOpts).close().getAwaitDuration shouldBe Duration("1h")

    val (missingOpts, _) = ScioContext.parseArguments[PipelineOptions](Array())
    ScioContext.apply(missingOpts).close().getAwaitDuration shouldBe Duration.Inf

    val (invalidOpts, _) = ScioContext.parseArguments[PipelineOptions](Array(s"--blockFor=foo"))
    the[IllegalArgumentException] thrownBy { ScioContext.apply(invalidOpts) } should have message
      s"blockFor param foo cannot be cast to type scala.concurrent.duration.Duration"
  }
}
