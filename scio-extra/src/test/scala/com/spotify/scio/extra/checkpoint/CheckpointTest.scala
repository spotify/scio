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

package com.spotify.scio.extra.checkpoint

import java.nio.file.Files

import com.spotify.scio.{ContextAndArgs, ScioMetrics}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.io.File
import scala.util.Try

object CheckpointMetrics {

  def runJob(checkpointArg: String, tempLocation: String = null): (Long, Long) = {
    val elemsBefore = ScioMetrics.counter("elemsBefore")
    val elemsAfter = ScioMetrics.counter("elemsAfter")

    val (sc, args) = ContextAndArgs(Array(s"--checkpoint=$checkpointArg") ++
      Option(tempLocation).map(e => s"--tempLocation=$e"))
    sc.checkpoint(args("checkpoint")) {
      sc.parallelize(1 to 10)
        .map { x => elemsBefore.inc(); x }
    }
    .map { x => elemsAfter.inc(); x }
    val r = sc.close().waitUntilDone()
    (Try(r.counter(elemsBefore).committed.get).getOrElse(0),
      r.counter(elemsAfter).committed.get)
  }
}

class CheckpointTest extends FlatSpec with Matchers {
  import CheckpointMetrics._

  "checkpoint" should "work on path" in {
    val tmpDir = Files.createTempDirectory("checkpoint-").resolve("checkpoint").toString
    runJob(tmpDir) shouldBe ((10L, 10L))
    runJob(tmpDir) shouldBe ((0L, 10L))
    File(tmpDir).deleteRecursively()
    runJob(tmpDir) shouldBe ((10L, 10L))
  }

  it should "work on name/file" in {
    val checkpointName = "c1"
    val tempLocation = Files.createTempDirectory("temp-location-").toString
    runJob(checkpointName, tempLocation) shouldBe ((10L, 10L))
    runJob(checkpointName, tempLocation) shouldBe ((0L, 10L))
    File(s"$tempLocation/$checkpointName").deleteRecursively()
    runJob(checkpointName, tempLocation) shouldBe ((10L, 10L))
  }

}
