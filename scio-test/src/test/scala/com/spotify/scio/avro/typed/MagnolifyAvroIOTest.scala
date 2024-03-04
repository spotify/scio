/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.avro.typed

import com.spotify.scio.avro.AvroIO
import com.spotify.scio.testing.ScioIOSpec

object MagnolifyAvroIOTest {
  case class Record(i: Int, s: String, r: List[String])
}

class MagnolifyAvroIOTest extends ScioIOSpec {
  import MagnolifyAvroIOTest._

  "TypedMagnolifyAvroIO" should "work with typed Avro" in {
    val xs = (1 to 100).map(x => Record(x, x.toString, (1 to x).map(_.toString).toList))
    testTap(xs)(_.saveAsTypedAvroFile(_))(".avro")
    testJobTest(xs)(AvroIO[Record])(_.typedAvroFile[Record](_))(_.saveAsTypedAvroFile(_))
  }
}
