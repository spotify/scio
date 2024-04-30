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

package com.spotify.scio.testing

import java.lang.reflect.InvocationTargetException
import com.spotify.scio.{ScioContext, ScioResult}
import com.spotify.scio.io.{KeyedIO, ScioIO}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.runners.PTransformOverride
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.testing.TestStream.{ElementEvent, Event}
import org.apache.beam.sdk.values.TimestampedValue
import org.apache.beam.sdk.{metrics => beam}

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._

/**
 * Set up a Scio job for end-to-end unit testing. To be used in a
 * [[com.spotify.scio.testing.PipelineSpec PipelineSpec]]. For example:
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

  private case class BuilderState(
    job: Either[Class[_], ScioContext => Any],
    cmdlineArgs: Array[String] = Array(),
    input: Map[String, JobInputSource[_]] = Map.empty,
    output: Map[String, SCollection[_] => Any] = Map.empty,
    distCaches: Map[DistCacheIO[_], _] = Map.empty,
    counters: Set[MetricsAssertion[beam.Counter, Long]] = Set.empty,
    distributions: Set[MetricsAssertion[beam.Distribution, beam.DistributionResult]] = Set.empty,
    gauges: Set[MetricsAssertion[beam.Gauge, beam.GaugeResult]] = Set.empty,
    transformOverrides: Set[PTransformOverride] = Set.empty,
    wasRunInvoked: Boolean = false
  )

  sealed private trait MetricsAssertion[M <: beam.Metric, V]
  final private case class SingleMetricAssertion[M <: beam.Metric, V](
    metric: M,
    assert: V => Any
  ) extends MetricsAssertion[M, V]

  final private case class AllMetricsAssertion[M <: beam.Metric, V](
    assert: Map[beam.MetricName, V] => Any
  ) extends MetricsAssertion[M, V]

  class Builder(private var state: BuilderState) {

    /** Test ID for input and output wiring. */
    val testId: String = state.job match {
      case Left(clazz) => TestUtil.newTestId(clazz)
      case Right(_)    => TestUtil.newTestId()
    }

    private[testing] def wasRunInvoked: Boolean = state.wasRunInvoked

    /** Feed command line arguments to the pipeline being tested. */
    def args(newArgs: String*): Builder = {
      state = state.copy(cmdlineArgs = (state.cmdlineArgs.toSeq ++ newArgs).toArray)
      this
    }

    /**
     * Feed an input in the form of a raw `Iterable[T]` to the pipeline being tested. Note that
     * `ScioIO[T]` must match the one used inside the pipeline, e.g. `AvroIO[MyRecord]("in.avro")`
     * with `sc.avroFile[MyRecord]("in.avro")`.
     */
    def input[T: Coder](io: ScioIO[T], value: Iterable[T]): Builder = {
      val source = io match {
        case kio: KeyedIO[T @unchecked] =>
          implicit val keyCoder: Coder[kio.KeyT] = kio.keyCoder
          IterableInputSource(value.map(x => kio.keyBy(x) -> x))
        case _ =>
          IterableInputSource(value)
      }
      input(io, source)
    }

    /**
     * Feed an input in the form of a `PTransform[PBegin, PCollection[T]]` to the pipeline being
     * tested. Note that `PTransform` inputs may not be supported for all `ScioIO[T]` types.
     */
    def inputStream[T: Coder](io: ScioIO[T], stream: TestStream[T]): Builder = {
      val source = io match {
        case kio: KeyedIO[T @unchecked] =>
          implicit val keyCoder: Coder[kio.KeyT] = kio.keyCoder
          val bCoder = CoderMaterializer.beamWithDefault(Coder[(kio.KeyT, T)])
          val kvEvents = stream.getEvents.asScala.map {
            case elemEvent: ElementEvent[T @unchecked] =>
              val values = elemEvent.getElements.asScala.map { e =>
                val value = e.getValue
                val ts = e.getTimestamp
                TimestampedValue.of[(kio.KeyT, T)](kio.keyBy(value) -> value, ts)
              }
              ElementEvent.add(values.asJava)
            case e => e.asInstanceOf[Event[(kio.KeyT, T)]]
          }
          TestStreamInputSource(TestStream.fromRawEvents(bCoder, kvEvents.asJava))
        case _ =>
          TestStreamInputSource(stream)
      }
      input(io, source)
    }

    private def input(io: ScioIO[_], value: JobInputSource[_]): Builder = {
      require(!state.input.contains(io.testId), "Duplicate test input: " + io.testId)
      state = state.copy(input = state.input + (io.testId -> value))
      this
    }

    /**
     * Evaluate an output of the pipeline being tested. Note that `TestIO[T]` must match the one
     * used inside the pipeline, e.g. `AvroIO[MyRecord]("out.avro")` with
     * `data.saveAsAvroFile("out.avro")` where `data` is of type `SCollection[MyRecord]`.
     *
     * @param assertion
     *   assertion for output data. See [[SCollectionMatchers]] for available matchers on an
     *   [[com.spotify.scio.values.SCollection SCollection]].
     */
    def output[T](io: ScioIO[T])(assertion: SCollection[T] => Any): Builder = {
      require(!state.output.contains(io.testId), "Duplicate test output: " + io.testId)
      state = state.copy(
        output = state.output + (io.testId -> assertion.asInstanceOf[SCollection[_] => AnyVal])
      )
      this
    }

    /**
     * Feed an distributed cache to the pipeline being tested. Note that `DistCacheIO[T]` must match
     * the one used inside the pipeline, e.g. `DistCacheIO[Set[String]]("dc.txt")` with
     * `sc.distCache("dc.txt")(f => scala.io.Source.fromFile(f).getLines().toSet)`.
     *
     * @param value
     *   mock value, must be serializable.
     */
    def distCache[T](key: DistCacheIO[T], value: T): Builder = {
      require(!state.distCaches.contains(key), "Duplicate test dist cache: " + key)
      state = state.copy(distCaches = state.distCaches + (key -> (() => value)))
      this
    }

    /**
     * Feed an distributed cache to the pipeline being tested. Note that `DistCacheIO[T]` must match
     * the one used inside the pipeline, e.g. `DistCacheIO[Set[String]]("dc.txt")` with
     * `sc.distCache("dc.txt")(f => scala.io.Source.fromFile(f).getLines().toSet)`.
     *
     * @param initFn
     *   init function, must be serializable.
     */
    def distCacheFunc[T](key: DistCacheIO[T], initFn: () => T): Builder = {
      require(!state.distCaches.contains(key), "Duplicate test dist cache: " + key)
      state = state.copy(distCaches = state.distCaches + (key -> initFn))
      this
    }

    /**
     * Replace a PTransform in the pipeline being tested.
     *
     * @param xformOverride
     *   A [[org.apache.beam.sdk.runners.PTransformOverride PTransformOverride]]
     */
    def transformOverride(xformOverride: PTransformOverride): Builder = {
      state = state.copy(transformOverrides = state.transformOverrides + xformOverride)
      this
    }

    /**
     * Evaluate a [[org.apache.beam.sdk.metrics.Counter Counter]] in the pipeline being tested.
     * @param counter
     *   counter to be evaluated
     * @param assertion
     *   assertion for the counter result's committed value
     */
    def counter(counter: beam.Counter)(assertion: Long => Any): Builder = {
      require(
        !state.counters.exists {
          case a: SingleMetricAssertion[beam.Counter, Long] => a.metric == counter
          case _                                            => false
        },
        "Duplicate test counter: " + counter.getName
      )

      state = state.copy(
        counters = state.counters +
          SingleMetricAssertion(counter, assertion)
      )
      this
    }

    /**
     * Evaluate all [[org.apache.beam.sdk.metrics.Counter Counters]] in the pipeline being tested.
     * @param assertion
     *   assertion on the collection of all job counters' committed values
     */
    def counters(assertion: Map[beam.MetricName, Long] => Any): Builder = {
      state = state.copy(
        counters = state.counters +
          AllMetricsAssertion[beam.Counter, Long](assertion)
      )

      this
    }

    /**
     * Evaluate a [[org.apache.beam.sdk.metrics.Distribution Distribution]] in the pipeline being
     * tested.
     * @param distribution
     *   distribution to be evaluated
     * @param assertion
     *   assertion for the distribution result's committed value
     */
    def distribution(
      distribution: beam.Distribution
    )(assertion: beam.DistributionResult => Any): Builder = {
      require(
        !state.distributions.exists {
          case a: SingleMetricAssertion[beam.Distribution, beam.DistributionResult] =>
            a.metric == distribution
          case _ => false
        },
        "Duplicate test distribution: " + distribution.getName
      )

      state = state.copy(
        distributions = state.distributions +
          SingleMetricAssertion(distribution, assertion)
      )
      this
    }

    /**
     * Evaluate all [[org.apache.beam.sdk.metrics.Distribution Distributions]] in the pipeline being
     * tested.
     * @param assertion
     *   assertion on the collection of all job distribution results' committed values
     */
    def distributions(assertion: Map[beam.MetricName, beam.DistributionResult] => Any): Builder = {
      state = state.copy(
        distributions = state.distributions +
          AllMetricsAssertion[beam.Distribution, beam.DistributionResult](assertion)
      )

      this
    }

    /**
     * Evaluate a [[org.apache.beam.sdk.metrics.Gauge Gauge]] in the pipeline being tested.
     * @param gauge
     *   gauge to be evaluated
     * @param assertion
     *   assertion for the gauge result's committed value
     */
    def gauge(gauge: beam.Gauge)(assertion: beam.GaugeResult => Any): Builder = {
      require(
        !state.gauges.exists {
          case a: SingleMetricAssertion[beam.Gauge, beam.GaugeResult] =>
            a.metric == gauge
          case _ => false
        },
        "Duplicate test gauge: " + gauge.getName
      )

      state = state.copy(gauges = state.gauges + SingleMetricAssertion(gauge, assertion))
      this
    }

    /**
     * Evaluate all [[org.apache.beam.sdk.metrics.Gauge Gauges]] in the pipeline being tested.
     * @param assertion
     *   assertion on the collection of all job gauge results' committed values
     */
    def gauges(assertion: Map[beam.MetricName, beam.GaugeResult] => Any): Builder = {
      state = state.copy(
        gauges = state.gauges +
          AllMetricsAssertion[beam.Gauge, beam.GaugeResult](assertion)
      )

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
        state.distCaches,
        state.transformOverrides
      )

    /**
     * Tear down test wiring. Use this only if you have custom pipeline wiring and are bypassing
     * [[run]]. Make sure [[setUp]] is called before.
     */
    def tearDown(): Unit = {
      val metricsFn = (result: ScioResult) => {
        state.counters.foreach {
          case a: SingleMetricAssertion[beam.Counter, Long] =>
            a.assert(result.counter(a.metric).attempted)
          case a: AllMetricsAssertion[beam.Counter, Long] =>
            a.assert(result.allCounters.map(c => c._1 -> c._2.attempted))
        }
        state.gauges.foreach {
          case a: SingleMetricAssertion[beam.Gauge, beam.GaugeResult] =>
            a.assert(result.gauge(a.metric).attempted)
          case a: AllMetricsAssertion[beam.Gauge, beam.GaugeResult] =>
            a.assert(result.allGauges.map(c => c._1 -> c._2.attempted))
        }
        state.distributions.foreach {
          case a: SingleMetricAssertion[beam.Distribution, beam.DistributionResult] =>
            a.assert(result.distribution(a.metric).attempted)
          case a: AllMetricsAssertion[beam.Distribution, beam.DistributionResult] =>
            a.assert(result.allDistributions.map(c => c._1 -> c._2.attempted))
        }
      }
      TestDataManager.tearDown(testId, metricsFn)
    }

    /** Run the pipeline with test wiring. */
    def run(): Unit = {
      state = state.copy(wasRunInvoked = true)
      setUp()

      state.job match {
        case Left(clazz) =>
          try {
            clazz
              .getMethod("main", classOf[Array[String]])
              .invoke(null, state.cmdlineArgs :+ s"--appName=$testId")
          } catch {
            // InvocationTargetException stacktrace is noisy and useless
            case e: InvocationTargetException => throw e.getCause
            case NonFatal(e)                  => throw e
          }
        case Right(job) =>
          val sc = ScioContext.forTest(testId)
          job(sc)
          sc.run()
      }

      tearDown()
    }

    override def toString: String = {
      val sb = new StringBuilder()
      sb.append(s"JobTest[${state.job.fold(_.getName, _ => "_")}]")
      sb.append("(\n")
      if (state.cmdlineArgs.nonEmpty) sb.append(s"\targs: ${state.cmdlineArgs.mkString(" ")}\n")
      if (state.distCaches.nonEmpty)
        sb.append(s"\tdistCache: ${state.distCaches.keys.map(_.uri).mkString(" ")}\n")
      if (state.input.nonEmpty) sb.append(s"\tinputs: ${state.input.keys.mkString(", ")}\n")
      if (state.output.nonEmpty) sb.append(s"\toutputs: ${state.output.keys.mkString(", ")}\n")
      if (state.transformOverrides.nonEmpty)
        sb.append(s"\toutputs: ${state.transformOverrides.map(_.getMatcher).mkString(", ")}\n")
      sb.append(")\n")
      sb.result()
    }
  }

  /** Create a new JobTest.Builder instance. */
  def apply(className: String)(implicit bo: BeamOptions): Builder =
    new Builder(BuilderState(Left(Class.forName(className))))
      .args(bo.opts: _*)

  /** Create a new JobTest.Builder instance. */
  def apply[T: ClassTag](implicit bo: BeamOptions): Builder = {
    val className = ScioUtil.classOf[T].getName.replaceAll("\\$$", "")
    apply(className).args(bo.opts: _*)
  }

  /** Create a new JobTest.Builder instance. */
  def apply(job: ScioContext => Any)(implicit bo: BeamOptions): Builder =
    new Builder(BuilderState(Right(job))).args(bo.opts: _*)
}
