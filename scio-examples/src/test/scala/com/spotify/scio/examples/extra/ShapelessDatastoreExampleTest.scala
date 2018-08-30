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

import com.google.datastore.v1.client.DatastoreHelper.{makeKey, makeValue}
import com.google.datastore.v1.Entity
import com.spotify.scio.io._
import com.spotify.scio.testing._

class ShapelessDatastoreExampleTest extends PipelineSpec {

  val textIn = Seq("a b c d e", "a b a b")
  val wordCount = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val entities = wordCount.map { kv =>
    Entity.newBuilder()
      .setKey(makeKey(ShapelessDatastoreExample.kind, kv._1))
      .putProperties("word", makeValue(kv._1).build())
      .putProperties("count", makeValue(kv._2).build())
      .build()
  }
  val textOut = wordCount.map(kv => kv._1 + ": " + kv._2)

  "ShapelessDatastoreWriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.ShapelessDatastoreWriteExample.type]
      .args("--input=in.txt", "--output=project")
      .input(TextIO("in.txt"), textIn)
      .output(DatastoreIO("project"))(_ should containInAnyOrder (entities))
      .run()
  }

  "ShapelessDatastoreReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.ShapelessDatastoreReadExample.type]
      .args("--input=project", "--output=out.txt")
      .input(DatastoreIO("project"), entities)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (textOut))
      .run()
  }

}
