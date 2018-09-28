/*
 * Copyright 2018 Spotify AB.
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

import org.apache.beam.sdk.options.PipelineOptions
import org.scalatest.{FlatSpec, Matchers}

trait Opts extends PipelineOptions {
  def getFoo: String

  def setFoo(s: String)
}

class ScioContextTest extends FlatSpec with Matchers {

  "ScioContext.parseOptions" should "parse known options" in {
    val args = Array("--jobName=name", "--foo=bar")
    val opts = ScioContext.parseOptions[Opts](args)
    opts.getFoo shouldBe "bar"
    opts.getJobName shouldBe "name"
  }

  it should "throw if given unknown option" in {
    val args = Array("--jobName=name", "--foo=bar", "--unknown=value")
    an[IllegalArgumentException] should be thrownBy ScioContext.parseOptions[Opts](args)
  }
}
