/*
 * Copyright 2017 Spotify AB.
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

import java.net.URI

import com.spotify.scio.testing.ItUtils
import com.spotify.scio.util.ScioUtil
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.scalatest._

class ScioContextIT extends FlatSpec with Matchers {

  "ScioContext" should "have temp location for DataflowRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DataflowRunner])
    opts.as(classOf[GcpOptions]).setProject(ItUtils.project)
    verify(opts)
  }

  it should "support user defined temp location for DataflowRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DataflowRunner])
    opts.as(classOf[GcpOptions]).setProject(ItUtils.project)
    opts.setTempLocation(ItUtils.gcpTempLocation("scio-context-it"))
    verify(opts)
  }

  private def verify(options: PipelineOptions): Unit = {
    val pipeline = ScioContext(options).pipeline
    val tempLocation = pipeline.getOptions.getTempLocation
    val gcpTempLocation = pipeline.getOptions.as(classOf[GcpOptions]).getGcpTempLocation

    tempLocation should not be null
    gcpTempLocation should not be null
    tempLocation shouldBe gcpTempLocation
    ScioUtil.isGcsUri(new URI(gcpTempLocation)) shouldBe true
  }

}
