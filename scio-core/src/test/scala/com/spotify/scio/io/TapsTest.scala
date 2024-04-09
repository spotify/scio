/*
 * Copyright 2019 Spotify AB.
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

import java.nio.file.{Files, Path}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class TapsTest extends AnyFlatSpec with Matchers {
  val data: Seq[String] = Seq("a", "b", "c")

  private def tmpFile: Path = Files.createTempFile("taps-test-", ".txt")

  private def writeText(p: Path, data: Seq[String]): Unit = {
    val writer = Files.newBufferedWriter(p)
    data.foreach { s =>
      writer.write(s)
      writer.newLine()
    }
    writer.close()
  }

  "ImmediateTap" should "work with text file" in {
    TapsSysProps.Algorithm.value = "immediate"
    val f = tmpFile
    writeText(f, data)
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe true
    future.value.get.isSuccess shouldBe true
    Await.result(future, Duration.Inf).value.toSeq shouldBe data
    Files.delete(f)
  }

  it should "fail missing text file" in {
    TapsSysProps.Algorithm.value = "immediate"
    val f = tmpFile
    Files.delete(f)
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe true
    future.value.get.isSuccess shouldBe false
  }

  "PollingTap" should "work with text file" in {
    TapsSysProps.Algorithm.value = "polling"
    TapsSysProps.PollingInitialInterval.value = "1000"
    TapsSysProps.PollingMaximumAttempts.value = "1"
    val f = tmpFile
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe false
    writeText(f, data)

    val result = Await.result(future, 10.seconds)
    result.value.toSeq shouldBe data

    Files.delete(f)
  }

  it should "fail missing text file" in {
    TapsSysProps.Algorithm.value = "polling"
    TapsSysProps.PollingInitialInterval.value = "1000"
    TapsSysProps.PollingMaximumAttempts.value = "1"
    val f = tmpFile
    Files.delete(f)
    val future = Taps().textFile(f.toString)
    future.isCompleted shouldBe false
    val error = Await.result(future.failed, 10.seconds)
    error shouldBe a[TapNotAvailableException]
    error.getMessage shouldBe s"Text: $f"
  }
}
