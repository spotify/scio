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

package com.spotify.scio.tensorflow

import com.spotify.scio.testing._
import shapeless.datatype.tensorflow._

object TFExampleIOTest {
  case class Record(i: Int, s: String)

  val recordT: TensorFlowType[Record] = TensorFlowType[Record]
}

class TFExampleIOTest extends ScioIOSpec {

  import TFExampleIOTest._

  "TFEXampleIO" should "work" in {
    val xs = (1 to 100).map(x => recordT.toExample(Record(x, x.toString)))
    testTap(xs)(TFExampleIO(_))(_.tfRecordExampleFile(_))(_.saveAsTfExampleFile(_))(".tfrecords")
    testJobTest(xs)(TFExampleIO(_))(_.tfRecordExampleFile(_))(_.saveAsTfExampleFile(_))
  }

}
