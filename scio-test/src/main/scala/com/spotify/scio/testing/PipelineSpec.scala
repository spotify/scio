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

import org.scalatest.{FlatSpec, Matchers, ConfigMap, Status, Args}
import JobTest.BeamOptions

/**
  * Trait for unit testing pipelines.
  *
  * A simple test might look like this:
  * {{{
  * class SimplePipelineTest extends PipelineSpec {
  *   "A simple pipeline" should "sum integers" in {
  *     runWithContext { sc =>
  *       sc.parallelize(Seq(1, 2, 3)).sum should containSingleValue (6)
  *     }
  *   }
  * }
  * }}}
  */
trait PipelineSpec
    extends FlatSpec
    with Matchers
    with SCollectionMatchers
    with PipelineTestUtils
    with RunEnforcementJobTest {

  private val Beam = """beam\.(.*)""".r

  private var beamOpts: BeamOptions = _

  private val aliases =
    Map(
      "flink" ->
        List("runner" -> "FlinkRunner", "flinkMaster" -> "[local]"))

  private def getBeamOptions(m: ConfigMap): List[String] =
    m.collect { case (Beam(k), v) => k -> v }
      .flatMap { case (k, v) => aliases.getOrElse(k, List(k -> v)) }
      .map { case (k, v) => s"--$k=$v" }
      .toList

  implicit def beamOptions: BeamOptions = {
    assume(beamOpts != null)
    beamOpts
  }

  override def run(testName: Option[String], args: Args): Status = {
    if (beamOpts == null && !args.runTestInNewInstance && (expectedTestCount(args.filter) > 0)) {
      beamOpts = BeamOptions(getBeamOptions(args.configMap))
    }
    super.run(testName, args)
  }

}
