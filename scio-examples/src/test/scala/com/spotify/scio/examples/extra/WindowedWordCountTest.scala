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

package com.spotify.scio.examples.extra

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._
import org.joda.time.{Duration, Instant}

class WindowedWordCountTest extends PipelineSpec {
  private val baseTime = new Instant(0)

  "WindowedWordCount" should "work" in {
    JobTest[com.spotify.scio.examples.WindowedWordCount.type]
      .args(
        "--input=input.txt",
        s"--windowSize=PT0.1S", // 100 ms, in ISO-8601 standard used by Joda for Duration parsing
        "--outputGlobalWindow=true",
        "--output=output.txt"
      )
      .inputStream(
        TextIO("input.txt"),
        testStreamOf[String]
          .advanceWatermarkTo(baseTime)
          .addElements("a b b c")
          .advanceWatermarkTo(baseTime.plus(Duration.millis(150)))
          .addElements("b e")
          .advanceWatermarkToInfinity()
      )
      .output(TextIO("output.txt"))(
        _ should containInAnyOrder(Seq("(a,1)", "(b,2)", "(b,1)", "(c,1)", "(e,1)"))
      )
      .run()
  }
}
