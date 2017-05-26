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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing.{ObjectFileIO, PipelineSpec, TextIO}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.io.File

class CheckpointTest extends FlatSpec with Matchers {

  import com.spotify.scio.accumulators._

  private def runJob(checkpointArg: String,
                     tempLocation: String = null) = {
    val (sc, args) = ContextAndArgs(Array(s"--checkpoint=$checkpointArg") ++
      Option(tempLocation).map(e => s"--tempLocation=$e"))
    val elemsBefore = sc.sumAccumulator[Long]("elemsBefore")
    val elemsAfter = sc.sumAccumulator[Long]("elemsAfter")
    sc.checkpoint(args("checkpoint"))(sc.parallelize(1 to 10).accumulateCount(elemsBefore))
      .accumulateCount(elemsAfter)
    val r = sc.close().waitUntilDone()
    (r.accumulatorTotalValue(elemsBefore), r.accumulatorTotalValue(elemsAfter))
  }

  "checkpoint" should "work on path" in {
    val tmpDir = Files.createTempDirectory("checkpoint_dir").resolve("checkpoint").toString
    runJob(tmpDir) shouldBe (10L, 10L)
    runJob(tmpDir) shouldBe (0L, 10L)
    File(tmpDir).deleteRecursively()
    runJob(tmpDir) shouldBe (10L, 10L)
  }

  it should "work on name/file" in {
    val checkpointName = "c1"
    val tempLocation = Files.createTempDirectory("tempLocation").toString
    runJob(checkpointName, tempLocation) shouldBe (10L, 10L)
    runJob(checkpointName, tempLocation) shouldBe (0L, 10L)
    File(s"$tempLocation/$checkpointName").deleteRecursively()
    runJob(checkpointName, tempLocation) shouldBe (10L, 10L)
  }

}

object CheckpointJobTest {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val tenTo20 = sc.parallelize(10 to 20)
    val sideIn = sc.checkpoint("sum")(tenTo20.sum).asSingletonSideInput
    sc.checkpoint("1to10")(sc.parallelize(1 to 10))
      .withSideInputs(sideIn)
      .map{case (e, c) => c(sideIn) + e}
      .toSCollection
      .saveAsTextFile(args("output"))
    sc.close()
  }
}

class CheckpointJobTest extends PipelineSpec {

  "CheckpointJobTest" should "work" in {
    val s = (10 to 20).sum
    val tempLocation = Files.createTempDirectory("tempLocation")
    JobTest[CheckpointJobTest.type]
      .args("--output=output", s"--tempLocation=$tempLocation")
      .output(TextIO("output"))(_ should containInAnyOrder ((1 to 10).map(_ + s).map(_.toString)))
      .output[Int](ObjectFileIO(s"$tempLocation/1to10"))(_ should containInAnyOrder (1 to 10))
      .output[Int](ObjectFileIO(s"$tempLocation/sum"))(_ should containSingleValue (s))
      .run()
  }

}
