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

package com.spotify.scio.io

import java.io.File
import java.nio.file.{Files, Path}
import java.util.UUID

import org.scalatest._

class TapsTest extends FlatSpec with Matchers {

  val data = Seq("a", "b", "c")

  private def tmpFile: Path = new File(
    new File(sys.props("java.io.tmpdir")),
    "taps-test-" + UUID.randomUUID()).toPath

  private def writeText(p: Path, data: Seq[String]) = {
    val writer = Files.newBufferedWriter(p)
    data.foreach { s =>
      writer.write(s)
      writer.newLine()
    }
    writer.close()
  }

  "ImmediateTap" should "work with text file" in {
    sys.props(Taps.ALGORITHM_KEY) = "immediate"
    val f = tmpFile
    writeText(f, data)
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe true
    future.value.get.isSuccess shouldBe true
    future.waitForResult().value.toSeq shouldBe data
    Files.delete(f)
  }

  it should "fail missing text file" in {
    sys.props(Taps.ALGORITHM_KEY) = "immediate"
    val f = tmpFile
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe true
    future.value.get.isSuccess shouldBe false
  }

  "PollingTap" should "work with text file" in {
    sys.props(Taps.ALGORITHM_KEY) = "polling"
    sys.props(Taps.POLLING_INITIAL_INTERVAL_KEY) = "1000"
    sys.props(Taps.POLLING_MAXIMUM_ATTEMPTS_KEY) = "1"
    val f = tmpFile
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe false
    writeText(f, data)
    Thread.sleep(2000)
    future.isCompleted shouldBe true
    future.value.get.isSuccess shouldBe true
    future.waitForResult().value.toSeq shouldBe data
    Files.delete(f)
  }

  it should "fail missing text file" in {
    sys.props(Taps.ALGORITHM_KEY) = "polling"
    sys.props(Taps.POLLING_INITIAL_INTERVAL_KEY) = "1000"
    sys.props(Taps.POLLING_MAXIMUM_ATTEMPTS_KEY) = "1"
    val f = tmpFile
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe false
    Thread.sleep(5000)
    future.isCompleted shouldBe true
  }

}
