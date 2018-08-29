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

package com.spotify.scio.testing

import java.lang.reflect.InvocationTargetException

import com.spotify.scio.ScioResult
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.{metrics => bm}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * Set up a Scio job for end-to-end unit testing.
 * To be used in a [[com.spotify.scio.testing.PipelineSpec PipelineSpec]]. For example:
 *
 * {{{
 * import com.spotify.scio.testing._
 *
 * class WordCountTest extends PipelineSpec {
 *
 *   // Mock input data, mock distributed cache and expected result
 *   val inData = Seq("a b c d e", "a b a b")
 *   val distCache = Map(1 -> "Jan", 2 -> "Feb", 3 -> "Mar")
 *   val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")
 *
 *   // Test specification
 *   "WordCount" should "work" in {
 *     JobTest("com.spotify.scio.examples.WordCount")
 *     // Or the type safe version
 *     // JobTest[com.spotify.scio.examples.WordCount.type]
 *
 *       // Command line arguments
 *       .args("--input=in.txt", "--output=out.txt")
 *
 *       // Mock input data
 *       .input(TextIO("in.txt"), inData)
 *
 *       // Mock distributed cache
 *       .distCache(DistCacheIO("gs://dataflow-samples/samples/misc/months.txt"), distCache)
 *
 *       // Verify output
 *       .output(TextIO("out.txt")) { actual => actual should containInAnyOrder (expected) }
 *
 *       // Run job test
 *       .run()
 *   }
 * }
 * }}}
 */
object JobTest {

  case class BeamOptions(opts: List[String])

  private case class BuilderState(className: String,
                                  cmdlineArgs: Array[String] = Array(),
                                  input: Map[String, Iterable[_]] = Map.empty,
                                  output: Map[String, SCollection[_] => Unit] = Map.empty,
                                  distCaches: Map[DistCacheIO[_], _] = Map.empty,
                                  counters: Map[bm.Counter, Long => Unit] = Map.empty,
                                  // scalastyle:off line.size.limit
                                  distributions: Map[bm.Distribution, bm.DistributionResult => Unit] = Map.empty,
                                  // scalastyle:on line.size.limit
                                  gauges: Map[bm.Gauge, bm.GaugeResult => Unit] = Map.empty,
                                  wasRunInvoked: Boolean = false)

  class Builder(private var state: BuilderState) {

    /** Test ID for input and output wiring. */
    val testId: String = TestUtil.newTestId(state.className)

    private[testing] def wasRunInvoked: Boolean = state.wasRunInvoked

    /** Feed command line arguments to the pipeline being tested. */
    def args(newArgs: String*): Builder = {
      state = state.copy(cmdlineArgs = (state.cmdlineArgs.toSeq ++ newArgs).toArray)
      this
    }

    /**
     * Feed an input to the pipeline being tested. Note that `TestIO[T]` must match the one used
     * inside the pipeline, e.g. `AvroIO[MyRecord]("in.avro")` with
     * `sc.avroFile[MyRecord]("in.avro")`.
     */
    def input[T](io: ScioIO[T], value: Iterable[T]): Builder = {
      require(!state.input.contains(io.toString), "Duplicate test input: " + io.toString)
      state = state.copy(input = state.input + (io.testId -> value))
      this
    }

    /**
     * Evaluate an output of the pipeline being tested. Note that `TestIO[T]` must match the one
     * used inside the pipeline, e.g. `AvroIO[MyRecord]("out.avro")` with
     * `data.saveAsAvroFile("out.avro")` where `data` is of type `SCollection[MyRecord]`.
     *
     * @param assertion assertion for output data. See [[SCollectionMatchers]] for available
     *                  matchers on an [[com.spotify.scio.values.SCollection SCollection]].
     */
    def output[T](io: ScioIO[T])(assertion: SCollection[T] => Unit): Builder = {
      require(!state.output.contains(io.toString), "Duplicate test output: " + io.toString)
      state = state.copy(
        output = state.output + (io.testId -> assertion.asInstanceOf[SCollection[_] => Unit]))
      this
    }

    /**
     * Feed an distributed cache to the pipeline being tested. Note that `DistCacheIO[T]` must
     * match the one used inside the pipeline, e.g. `DistCacheIO[Set[String]]("dc.txt")` with
     * `sc.distCache("dc.txt")(f => scala.io.Source.fromFile(f).getLines().toSet)`.
     *
     * @param value mock value, must be serializable.
     */
    def distCache[T](key: DistCacheIO[T], value: T): Builder = {
      require(!state.distCaches.contains(key), "Duplicate test dist cache: " + key)
      state = state.copy(distCaches = state.distCaches + (key -> (() => value)))
      this
    }

    /**
     * Feed an distributed cache to the pipeline being tested. Note that `DistCacheIO[T]` must
     * match the one used inside the pipeline, e.g. `DistCacheIO[Set[String]]("dc.txt")` with
     * `sc.distCache("dc.txt")(f => scala.io.Source.fromFile(f).getLines().toSet)`.
     *
     * @param initFn init function, must be serializable.
     */
    def distCacheFunc[T](key: DistCacheIO[T], initFn: () => T): Builder = {
      require(!state.distCaches.contains(key), "Duplicate test dist cache: " + key)
      state = state.copy(distCaches = state.distCaches + (key -> initFn))
      this
    }

    /**
     * Evaluate a [[org.apache.beam.sdk.metrics.Counter Counter]] in the pipeline being tested.
     * @param counter counter to be evaluated
     * @param assertion assertion for the counter result's committed value
     */
    def counter(counter: bm.Counter)(assertion: Long => Unit): Builder = {
      require(!state.counters.contains(counter), "Duplicate test counter: " + counter.getName)
      state = state.copy(counters = state.counters + (counter -> assertion))
      this
    }

    /**
     * Evaluate a [[org.apache.beam.sdk.metrics.Distribution Distribution]] in the pipeline being
     * tested.
     * @param distribution distribution to be evaluated
     * @param assertion assertion for the distribution result's committed value
     */
    def distribution(distribution: bm.Distribution)
                    (assertion: bm.DistributionResult => Unit): Builder = {
      require(
        !state.distributions.contains(distribution),
        "Duplicate test distribution: " + distribution.getName)
      state = state.copy(distributions = state.distributions + (distribution -> assertion))
      this
    }

    /**
     * Evaluate a [[org.apache.beam.sdk.metrics.Gauge Gauge]] in the pipeline being tested.
     * @param gauge gauge to be evaluated
     * @param assertion assertion for the gauge result's committed value
     */
    def gauge(gauge: bm.Gauge)(assertion: bm.GaugeResult => Unit): Builder = {
      require(!state.gauges.contains(gauge), "Duplicate test gauge: " + gauge.getName)
      state = state.copy(gauges = state.gauges + (gauge -> assertion))
      this
    }

    /**
     * Set up test wiring. Use this only if you have custom pipeline wiring and are bypassing
     * [[run]]. Make sure [[tearDown]] is called afterwards.
     */
    def setUp(): Unit =
      TestDataManager.setup(
        testId,
        state.input,
        state.output,
        state.distCaches
      )


    /**
     * Tear down test wiring. Use this only if you have custom pipeline wiring and are bypassing
     * [[run]]. Make sure [[setUp]] is called before.
     */
    def tearDown(): Unit = {
      val metricsFn = (result: ScioResult) => {
        state.counters.foreach { case (k, v) => v(result.counter(k).committed.get) }
        state.distributions.foreach { case (k, v) => v(result.distribution(k).committed.get) }
        state.gauges.foreach { case (k, v) => v(result.gauge(k).committed.get) }
      }
      TestDataManager.tearDown(testId, metricsFn)
    }

    /** Run the pipeline with test wiring. */
    def run(): Unit = {
      state = state.copy(wasRunInvoked = true)
      setUp()

      try {
        Class
          .forName(state.className)
          .getMethod("main", classOf[Array[String]])
          .invoke(null, state.cmdlineArgs :+ s"--appName=$testId")
      } catch {
        // InvocationTargetException stacktrace is noisy and useless
        case e: InvocationTargetException => throw e.getCause
        case NonFatal(e) => throw e
      }

      tearDown()
    }

    override def toString: String =
      s"""|JobTest[${state.className}](
          |\targs: ${state.cmdlineArgs.mkString(" ")}
          |\tdistCache: ${state.distCaches}
          |\tinputs: ${state.input.mkString(", ")}""".stripMargin

  }

  /** Create a new JobTest.Builder instance. */
  def apply(className: String)(implicit bm: BeamOptions): Builder =
    new Builder(BuilderState(className))
      .args(bm.opts:_*)

  /** Create a new JobTest.Builder instance. */
  def apply[T: ClassTag](implicit bm: BeamOptions): Builder = {
    val className=  ScioUtil.classOf[T].getName.replaceAll("\\$$", "")
    apply(className).args(bm.opts:_*)
  }

}
