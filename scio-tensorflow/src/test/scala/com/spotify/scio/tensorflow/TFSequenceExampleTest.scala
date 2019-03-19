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

package com.spotify.scio.tensorflow

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing.PipelineSpec

object SequenceExamplesJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    sc.parallelize(MetadataSchemaTest.sequenceExamples)
      .saveAsTfRecordFile(args("output"))
    sc.close()
  }
}

class TFSequenceExampleTest extends PipelineSpec {

  "SequenceExamplesJob" should "work" in {
    JobTest[ExamplesJobV2.type]
      .args("--output=out")
      .output(TFExampleIO("out"))(_ should haveSize(2))
      .run()
  }
}
