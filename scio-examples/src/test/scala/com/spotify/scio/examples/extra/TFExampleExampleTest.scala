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

package com.spotify.scio.examples.extra

import com.spotify.scio.io._
import com.spotify.scio.tensorflow.TFExampleIO
import com.spotify.scio.testing._

class TFExampleExampleTest extends PipelineSpec {
  import WordCountFeatureSpec._
  import shapeless.datatype.tensorflow._

  val input = Seq("foo", "bar", "foo")
  val output = Seq(WordCountFeatures(3.0f, 2.0f), WordCountFeatures(3.0f, 1.0f))
    .map(featuresType.toExample(_))

  "TFExampleExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.TFExampleExample.type]
      .args("--input=in", "--output=out")
      .input(TextIO("in"), input)
      .output(TFExampleIO("out"))(_ should containInAnyOrder (output))
      .run()
  }

  it should "work with custom feature desc path" in {
    JobTest[com.spotify.scio.examples.extra.TFExampleExample.type]
      .args("--input=in", "--output=out")
      .input(TextIO("in"), input)
      .output(TFExampleIO("out"))(_ should containInAnyOrder (output))
      .run()
  }

}
