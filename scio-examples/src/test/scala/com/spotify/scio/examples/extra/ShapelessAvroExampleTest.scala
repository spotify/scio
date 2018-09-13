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
import org.apache.avro.generic.{GenericData, GenericRecord}

class ShapelessAvroExampleTest extends PipelineSpec {

  import ShapelessAvroExample._

  val textIn = Seq("a b c d e", "a b a b")
  val wordCount = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val records: Seq[GenericRecord] = wordCount.map { kv =>
    val r = new GenericData.Record(wordCountSchema)
    r.put("word", kv._1)
    r.put("count", kv._2)
    r
  }
  val textOut = wordCount.map(kv => kv._1 + ": " + kv._2)

  "ShapelessAvroWriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.ShapelessAvroWriteExample.type]
      .args("--input=in.txt", "--output=wc.avro")
      .input(TextIO("in.txt"), textIn)
      .output(AvroIO[GenericRecord]("wc.avro"))(_ should containInAnyOrder (records))
      .run()
  }

  "ShapelessAvroReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.ShapelessAvroReadExample.type]
      .args("--input=wc.avro", "--output=out.txt")
      .input(AvroIO[GenericRecord]("wc.avro"), records)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (textOut))
      .run()
  }

}
