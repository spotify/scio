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

import com.spotify.scio.bigquery._
import com.spotify.scio.testing._

class TableRowJsonInOutTest extends PipelineSpec {

  val input = Seq(
    TableRow("field1" -> "str1", "field2" -> 100),
    TableRow("field1" -> "str2", "field2" -> 200)
  )

  "TableRowJsonInOut" should "work" in {
    JobTest[com.spotify.scio.examples.extra.TableRowJsonInOut.type]
      .args("--input=in.json", "--output=out.json")
      .input(TableRowJsonIO("in.json"), input)
      .output(TableRowJsonIO("out.json"))(_ should containInAnyOrder (input))
      .run()
  }

}
