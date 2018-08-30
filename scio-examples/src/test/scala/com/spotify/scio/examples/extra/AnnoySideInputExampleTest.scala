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
import com.spotify.scio.testing._

class AnnoySideInputExampleTest extends PipelineSpec {
  "AnnoySideInputExample" should "work" in {
    val temp = java.nio.file.Files.createTempDirectory("annoy-side-input-example-")
    temp.toFile.deleteOnExit

    val expected = Seq.fill(100)("1.0")
    JobTest[com.spotify.scio.examples.extra.AnnoySideInputExample.type]
      .args(
        "--output=out.txt",
        s"--tempLocation=$temp"
      )
      .output(TextIO("out.txt"))(_ should containInAnyOrder(expected))
      .run()
  }
}
