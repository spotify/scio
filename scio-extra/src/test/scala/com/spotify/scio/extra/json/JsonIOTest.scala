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
import com.spotify.scio.testing._
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._
import scala.io.Source


object JsonIOTest {
  case class Record(i: Int, s: String, o: Option[Int])
}

class JsonIOTest extends ScioIOSpec with TapSpec {

  import JsonIOTest._

  private val xs = (1 to 100).map(x => Record(x, x.toString, if (x % 2 == 0) Some(x) else None))

  "JsonIO" should "work" in {
    testTap(xs)(JsonIO(_))(_.jsonFile(_))(_.saveAsJsonFile(_))(".json")
    testJobTest(xs)(JsonIO(_))(_.jsonFile(_))(_.saveAsJsonFile(_))
  }

  it should "support custom printer" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(xs)
        .saveAsJsonFile(dir.getPath, printer = Printer.noSpaces.copy(dropNullValues = true))
    }
    verifyTap(t, xs.toSet)
    val result = Files.list(dir.toPath).iterator().asScala
      .flatMap(p => Source.fromFile(p.toFile).getLines())
      .toSeq
    val expected = (1 to 100).map { x =>
      s"""{"i":$x,"s":"$x"${if (x % 2 == 0) s""","o":$x""" else ""}}"""
    }
    result should contain theSameElementsAs expected
    FileUtils.deleteDirectory(dir)
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
