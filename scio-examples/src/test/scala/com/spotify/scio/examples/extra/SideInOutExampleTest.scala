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

package com.spotify.scio.examples.extra

import com.spotify.scio.io._
import com.spotify.scio.testing._

class SideInOutExampleTest extends PipelineSpec {

  val inData = Seq("The quick brown fox jumps over the lazy dog.")

  "SideInOutExample" should "work" in {
    JobTest[SideInOutExample.type]
      .args(
        "--input=in.txt", "--stopWords=stop.txt",
        "--output1=out1.txt", "--output2=out2.txt", "--output3=out3.txt", "--output4=out4.txt")
      .input(TextIO("in.txt"), inData)
      .input(TextIO("stop.txt"), Seq("the"))
      .output(TextIO("out1.txt"))(_ should containInAnyOrder (Seq.empty[String]))
      .output(TextIO("out2.txt"))(_ should containInAnyOrder (Seq.empty[String]))
      .output(TextIO("out3.txt"))(_ should containInAnyOrder (Seq("dog: 1", "fox: 1")))
      .output(TextIO("out4.txt")) {
        _ should containInAnyOrder (Seq("brown: 1", "jumps: 1", "lazy: 1", "over: 1", "quick: 1"))
      }
      .run()
  }

}
