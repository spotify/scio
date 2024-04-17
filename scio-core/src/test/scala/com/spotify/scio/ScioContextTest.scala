/*
 * Copyright 2019 Spotify AB.
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
import com.spotify.scio.io.TextIO
import com.spotify.scio.metrics.Metrics
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing.{PipelineSpec, TestValidationOptions}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.testing.TestUtil

import java.nio.charset.StandardCharsets
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create

import scala.concurrent.duration.Duration
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.Validation.Required

import scala.jdk.CollectionConverters._

class ScioContextTest extends PipelineSpec {
  "ScioContext" should "support pipeline" in {
    val pipeline = ScioContext().pipeline
    val p = pipeline.apply(Create.of(List(1, 2, 3).asJava))
    PAssert.that(p).containsInAnyOrder(List(1, 2, 3).asJava)
    pipeline.run()
  }

  it should "have temp location for default runner" in {
    val sc = ScioContext()
    sc.prepare()
    val opts = sc.options
    opts.getTempLocation should not be null
  }

  it should "have temp location for DirectRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectRunner])
    val sc = ScioContext(opts)
    sc.prepare()
    sc.options.getTempLocation should not be null
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

    sc.run()
    output.exists() shouldBe true
    output.delete()
  }

  it should "[io] create local output directory on close()" in {
    val output = Files.createTempDirectory("scio-output-").toFile
    output.delete()

    val sc = ScioContext()
    val textIO = TextIO(output.getAbsolutePath)
    sc.parallelize(Seq("a", "b", "c")).write(textIO)(TextIO.DefaultWriteParam)
    output.exists() shouldBe false

    sc.run()
    output.exists() shouldBe true
    output.delete()
  }

  it should "support save metrics on close for finished pipeline" in {
    val metricsFile = Files.createTempFile("scio-metrics-dump-", ".json").toFile
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectRunner])
    opts.as(classOf[ScioOptions]).setMetricsLocation(metricsFile.toString)
    val sc = ScioContext(opts)
    sc.run().waitUntilFinish() // block non-test runner

    val mapper = ScioUtil.getScalaJsonMapper

    val metrics = mapper.readValue(metricsFile, classOf[Metrics])
    metrics.version shouldBe BuildInfo.version
  }

  it should "fail to run() on closed context" in {
    val sc = ScioContext()
    sc.run()
    the[IllegalArgumentException] thrownBy {
      sc.run()
    } should have message "requirement failed: Pipeline cannot be modified once ScioContext has been executed"
  }

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
      Array(s"--optionsFile=${optionsFile.getAbsolutePath}")
    )
    arg("foo") shouldBe "bar"
  }

  it should "invalidate options where required arguments are missing" in {
    assertThrows[IllegalArgumentException] {
      ScioContext.parseArguments[TestValidationOptions](Array("--foo=bar"), true)
    }
  }

  it should "parse valid, invalid, and missing blockFor argument passed from command line" in {
    val (validOpts, _) =
      ScioContext.parseArguments[PipelineOptions](Array(s"--blockFor=1h"))
    ScioContext.apply(validOpts).awaitDuration shouldBe Duration("1h")

    val (missingOpts, _) = ScioContext.parseArguments[PipelineOptions](Array())
    ScioContext.apply(missingOpts).awaitDuration shouldBe Duration.Inf

    val (invalidOpts, _) =
      ScioContext.parseArguments[PipelineOptions](Array(s"--blockFor=foo"))
    the[IllegalArgumentException] thrownBy { ScioContext.apply(invalidOpts) } should have message
      s"blockFor param foo cannot be cast to type scala.concurrent.duration.Duration"
  }

  it should "truncate app arguments when they are overly long" in {
    val longArg = "--argument=" + ("a" * 55000)
    val (opts, _) = ScioContext.parseArguments[ScioOptions](Array(longArg))
    def numBytes(s: String): Int = s.getBytes(StandardCharsets.UTF_8.name).length
    val expectedNumBytes = 50000 + numBytes(" [...]")

    numBytes(opts.getAppArguments) shouldBe expectedNumBytes
  }

  behavior of "Counter initialization in ScioContext"
  it should "initialize Counters which are registered by name" in {
    val sc = ScioContext()
    sc.initCounter(name = "named-counter")
    val res = sc.run().waitUntilDone()

    val actualCommitedCounterValue = res
      .counter(ScioMetrics.counter(name = "named-counter"))
      .committed

    actualCommitedCounterValue shouldBe Some(0)
  }

  it should "initialize Counters which are registered by name and namespace" in {
    val sc = ScioContext()
    sc.initCounter(namespace = "ns", name = "name-spaced-counter")
    val res = sc.run().waitUntilDone()

    val actualCommitedCounterValue = res
      .counter(ScioMetrics.counter(namespace = "ns", name = "name-spaced-counter"))
      .committed

    actualCommitedCounterValue shouldBe Some(0)
  }

  it should "initialize Counters which are registered" in {
    val scioCounter = ScioMetrics.counter(name = "some-counter")
    val sc = ScioContext()
    sc.initCounter(scioCounter)
    val res = sc.run().waitUntilDone()

    val actualCommitedCounterValue = res
      .counter(scioCounter)
      .committed

    actualCommitedCounterValue shouldBe Some(0)
  }

  it should "support wrapped root-level transforms" in {
    val sc = ScioContext()
    val scioCounter = ScioMetrics.counter(name = "all-map-ops-counter")
    sc.initCounter(scioCounter)

    sc.transform("Transform1")(_.parallelize(1 to 10).tap(_ => scioCounter.inc()))
    sc.transform("Transform2")(_.parallelize(11 to 20).tap(_ => scioCounter.inc()))

    sc.run()
      .waitUntilDone()
      .counter(scioCounter)
      .committed shouldBe Some(20)
  }

  "PipelineOptions" should "propagate" in {
    trait Options extends DataflowPipelineOptions {
      @Required
      def getStringValue: String
      def setStringValue(value: String): Unit
    }

    val (opts, _) = ScioContext.parseArguments[Options](
      // test appName will switch ScioContext into test mode
      Array("--stringValue=foobar", s"--appName=${TestUtil.newTestId()}", "--project=dummy"),
      withValidation = true
    )
    val sc = ScioContext(opts)
    val internalOptions =
      sc.parallelize(Seq(1, 2, 3, 4))
        .map(_ + 1)
        .internal
        .getPipeline()
        .getOptions()
        .as(classOf[Options])

    internalOptions.getStringValue shouldBe "foobar"
  }

  it should "#1323: generate unique SCollection names" in {
    val options = PipelineOptionsFactory.create()
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.ERROR)
    val sc = ScioContext(options)

    val s1 = sc.empty[(String, Int)]()
    val s2 = sc.empty[(String, Double)]()
    s1.join(s2)

    noException shouldBe thrownBy(sc.run())
  }

  "RunnerContext" should "include ~/.m2, ~/.ivy2, ~/.cache/coursier, and ~/.sbt/boot/ dirs, but not other env dirs" in {
    val userDir = sys.props("user.home").replace("\\", "/")
    val paths1 = List(
      s"$userDir/.m2/repository/com/spotify/foo/0.0.1/foo-0.0.1.jar",
      s"$userDir/.ivy2/local/com/spotify/foo/0.0.1/jars/foo-0.0.1.jar",
      s"$userDir/.cache/coursier/v1/https/repo1.maven.org/maven2/com/spotify/foo/0.0.1/foo-0.0.1.jar",
      s"$userDir/.sbt/boot/scala-2.12.14/lib/foo.jar"
    )
    val paths2 = paths1 :+ s"$userDir/.env/a/b/c/foo-0.0.1.jar"

    paths1.filterNot(RunnerContext.isNonRepositoryEnvDir) should contain theSameElementsAs paths1
    paths2.filterNot(RunnerContext.isNonRepositoryEnvDir) should contain theSameElementsAs paths1
  }

}
