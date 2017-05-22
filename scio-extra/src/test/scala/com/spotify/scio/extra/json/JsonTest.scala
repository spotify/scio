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

package com.spotify.scio.extra.json

import com.spotify.scio._
import com.spotify.scio.io.TapSpec
import com.spotify.scio.util.ScioUtil
import org.apache.commons.io.FileUtils

object JsonJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    import JsonTest._
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.jsonFile[Record](args("input"))
      .flatMap(_.right.toOption)
      .saveAsJsonFile(args("output"))
    sc.close()
  }
}

object JsonTest {
  case class Record(i: Int, s: String)
}

class JsonTest extends TapSpec {

  import JsonTest._

  private val data = Seq(1, 2, 3).map(x => Record(x, x.toString))

  "Future" should "support saveAsJsonFile" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(data)
        .saveAsJsonFile(dir.getPath)
    }
    verifyTap(t, data.toSet)
  }

  "JobTest" should "pass correct JsonIO" in {
    JobTest[JsonJob.type]
      .args("--input=in.json", "--output=out.json")
      .input(JsonIO("in.json"), data)
      .output[Record](JsonIO("out.json"))(_ should containInAnyOrder (data))
      .run()
  }

  it should "handle invalid JSON" in {
    val badData = Seq(
      """{"i":1, "s":hello}""",
      """{"i":1}""",
      """{"s":"hello"}""",
      """{"i":1, "s":1}""",
      """{"i":"hello", "s":1}""")
    val dir = tmpDir
    runWithFileFuture {
      _.parallelize(badData).saveAsTextFile(dir.getPath)
    }
    val t = runWithFileFuture {
      _
        .jsonFile[Record](ScioUtil.addPartSuffix(dir.getPath))
        .flatMap(_.left.toOption.map(_.input))
        .materialize
    }
    verifyTap(t, badData.toSet)
    FileUtils.deleteDirectory(dir)
  }

}
