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

import com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.{DataflowPipelineRunner, DirectPipelineRunner}
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.common.collect.Lists
import com.spotify.scio.testing.PipelineSpec
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

  it should "have temp directory for default runner" in {
    val pipeline = ScioContext().pipeline
    pipeline.getOptions.getTempLocation should not be null
  }

  it should "have temp directory for default InProcessPipelineRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[InProcessPipelineRunner])
    val pipeline = ScioContext(opts).pipeline
    pipeline.getOptions.getTempLocation should not be null
  }

  it should "have temp directory for default DirectPipelineRunner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectPipelineRunner])
    val pipeline = ScioContext(opts).pipeline
    pipeline.getOptions.getTempLocation should not be null
  }

  it should "use user specified temp directory" in {
    val expected = "/expected"
    val opts = PipelineOptionsFactory.create()
    opts.setTempLocation(expected)
    val pipeline = ScioContext(opts).pipeline
    pipeline.getOptions.getTempLocation shouldEqual expected
  }

  it should "fail without temp/staging dir for DataflowPipelineRunner " in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DataflowPipelineRunner])
    val dfOpts = opts.as(classOf[DataflowPipelineOptions])
    dfOpts.setProject("foobar")
    val sc = ScioContext(dfOpts)
    val e = intercept[RuntimeException] { sc.pipeline }
    ExceptionUtils.getFullStackTrace(e) should
      include ("at least one of tempLocation or stagingLocation must be set.")
  }

}
