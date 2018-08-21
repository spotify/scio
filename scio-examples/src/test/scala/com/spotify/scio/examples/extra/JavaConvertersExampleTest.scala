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

package com.spotify.scio.examples.extra

import com.spotify.scio.nio.CustomIO
import com.spotify.scio.testing._

class JavaConvertersExampleTest extends PipelineSpec {

  val output = "gs://bucket/path/to/file"
  val expected: List[String] = (1 to 10).toList.map(_.toString)
  val io = CustomIO[String](output)

  "JavaConverters" should "convert filename prefix String to ResourceId" in {
    JobTest[com.spotify.scio.examples.extra.JavaConvertersExample.type]
      .args(s"--output=$output", "--converter=String#toResourceId")
      .output(io)(_ should containInAnyOrder (expected))
      .run()
  }

  it should "convert filename prefix String to StaticValueProvider[String]" in {
    JobTest[com.spotify.scio.examples.extra.JavaConvertersExample.type]
      .args(s"--output=$output", "--converter=String#toFilenamePolicy")
      .output(io)(_ should containInAnyOrder (expected))
      .run()
  }

  it should "convert filename prefix String to DefaultFilenamePolicy" in {
    JobTest[com.spotify.scio.examples.extra.JavaConvertersExample.type]
      .args(s"--output=$output", "--converter=String#toStaticValueProvider")
      .output(io)(_ should containInAnyOrder (expected))
      .run()
  }

  it should "convert FilenamePolicy case class to DefaultFilenamePolicy" in {
    JobTest[com.spotify.scio.examples.extra.JavaConvertersExample.type]
      .args(s"--output=$output", "--converter=FilenamePolicy#toJava")
      .output(io)(_ should containInAnyOrder (expected))
      .run()
  }

}
