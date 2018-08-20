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

import java.nio.file.Files

import io.circe.Printer
import com.spotify.scio._
import com.spotify.scio.io.TapSpec
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._
import scala.io.Source

object JsonJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    import JsonTest._
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.jsonFile[Record](args("input"))
      .saveAsJsonFile(args("output"))
    sc.close()
  }
}

object JsonTest {
  case class Record(i: Int, s: String, o: Option[Int])
}

class JsonTest extends TapSpec {

  import JsonTest._

  private val data = Seq(1, 2, 3).map(x => Record(x, x.toString, if (x % 2 == 0) Some(x) else None))

  "Future" should "support saveAsJsonFile" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(data)
        .saveAsJsonFile(dir.getPath)
    }
    verifyTap(t, data.toSet)
    FileUtils.deleteDirectory(dir)
  }

  it should "support custom printer" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(data)
        .saveAsJsonFile(dir.getPath, printer = Printer.noSpaces.copy(dropNullValues = true))
    }
    verifyTap(t, data.toSet)
    val result = Files.list(dir.toPath).iterator().asScala
      .flatMap(p => Source.fromFile(p.toFile).getLines())
      .toSeq
    val expected = Seq(
      """{"i":1,"s":"1"}""",
      """{"i":2,"s":"2","o":2}""",
      """{"i":3,"s":"3"}"""
    )
    result should contain theSameElementsAs expected
    FileUtils.deleteDirectory(dir)
  }

  "JobTest" should "pass correct JsonIO" in {
    JobTest[JsonJob.type]
      .args("--input=in.json", "--output=out.json")
      .input(JsonIO[Record]("in.json"), data)
      .output(JsonIO[Record]("out.json"))(_ should containInAnyOrder (data))
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

    val sc = ScioContext()
    sc.jsonFile[Record](ScioUtil.addPartSuffix(dir.getPath))
    // scalastyle:off no.whitespace.before.left.bracket
    a [PipelineExecutionException] should be thrownBy { sc.close() }
    // scalastyle:on no.whitespace.before.left.bracket
    FileUtils.deleteDirectory(dir)
  }

}
