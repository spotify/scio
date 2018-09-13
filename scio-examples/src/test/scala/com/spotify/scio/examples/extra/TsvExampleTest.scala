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

import com.spotify.scio.testing._

class TsvExampleTest extends PipelineSpec {

  val inData = Seq("a b c d e", "a b a b", "")
  val tsvData = Seq("a\t3", "b\t3", "c\t1", "d\t1", "e\t1")

  "TsvExample" should "write a TSV data" in {
    JobTest[TsvExampleWrite.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(CustomIO[String]("out.txt"))(_ should containInAnyOrder (tsvData))
      .run()
  }

  it should "read a TSV data" in {
    JobTest[TsvExampleRead.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), tsvData)
      .output(TextIO("out.txt"))(_ should containSingleValue ("9"))
      .run()
  }

}
