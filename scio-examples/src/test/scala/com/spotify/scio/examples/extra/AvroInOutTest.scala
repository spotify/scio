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

import com.spotify.scio.avro._
import com.spotify.scio.io._
import com.spotify.scio.testing._

import scala.collection.JavaConverters._

class AvroInOutTest extends PipelineSpec {

  val input = Seq(
    new TestRecord(1, 0L, 0F, 1000.0, false, "Alice", List[CharSequence]("a").asJava),
    new TestRecord(2, 0L, 0F, 1500.0, false, "Bob", List[CharSequence]("b").asJava))

  val expected = Seq(
    new Account(1, "checking", "Alice", 1000.0),
    new Account(2, "checking", "Bob", 1500.0))

  "AvroInOut" should "work" in {
    JobTest[com.spotify.scio.examples.extra.AvroInOut.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(AvroIO("in.avro"), input)
      .output(AvroIO[Account]("out.avro"))(_ should containInAnyOrder (expected))
      .run()
  }

}
