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

import com.spotify.scio.avro.AvroIO
import com.spotify.scio.io._
import com.spotify.scio.testing._

class MagnolifyAvroExampleTest extends PipelineSpec {
  import MagnolifyAvroExample._

  val textIn: Seq[String] = Seq("a b c d e", "a b a b")
  val wordCount: Seq[(String, Long)] = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val records: Seq[WordCount] = wordCount.map { case (word, count) => WordCount(word, count) }
  val textOut: Seq[String] = wordCount.map(kv => kv._1 + ": " + kv._2)

  "MagnolifyAvroWriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.MagnolifyAvroWriteExample.type]
      .args("--input=in.txt", "--output=wc.avro")
      .input(TextIO("in.txt"), textIn)
      .output(AvroIO[WordCount]("wc.avro"))(coll => coll should containInAnyOrder(records))
      .run()
  }

  "MagnolifyAvroReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.MagnolifyAvroReadExample.type]
      .args("--input=wc.avro", "--output=out.txt")
      .input(AvroIO[WordCount]("wc.avro"), records)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(textOut))
      .run()
  }
}
