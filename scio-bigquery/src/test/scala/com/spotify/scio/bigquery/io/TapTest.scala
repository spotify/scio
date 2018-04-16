/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.bigquery.io

import java.io._
import java.util.UUID

import com.spotify.scio.io.Tap
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.commons.io.FileUtils

import scala.concurrent.Future
import scala.reflect.ClassTag

trait TapSpec extends PipelineSpec {
  def verifyTap[T: ClassTag](tap: Tap[T], expected: Set[T]): Unit = {
    SerializableUtils.ensureSerializable(tap)
    tap.value.toSet shouldBe expected
    val sc = ScioContext()
    tap.open(sc) should containInAnyOrder (expected)
    sc.close().waitUntilFinish()  // block non-test runner
  }

  def runWithInMemoryFuture[T](fn: ScioContext => Future[Tap[T]]): Tap[T] =
    runWithFuture(ScioContext.forTest())(fn)

  def runWithFileFuture[T](fn: ScioContext => Future[Tap[T]]): Tap[T] =
    runWithFuture(ScioContext())(fn)

  def runWithFuture[T](sc: ScioContext)(fn: ScioContext => Future[Tap[T]]): Tap[T] = {
    val f = fn(sc)
    sc.close().waitUntilFinish()  // block non-test runner
    f.waitForResult()
  }

  def tmpDir: File = new File(
    new File(sys.props("java.io.tmpdir")),
    "scio-test-" + UUID.randomUUID())
}

class TapTest extends TapSpec {
  it should "support saveAsTableRowJsonFile" in {
    def newTableRow(i: Int): TableRow = TableRow(
      "int_field" -> 1 * i,
      "long_field" -> 1L * i,
      "float_field" -> 1F * i,
      "double_field" -> 1.0 * i,
      "boolean_field" -> "true",
      "string_field" -> "hello")

    val dir = tmpDir
    // Compare .toString versions since TableRow may not round trip
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newTableRow)
        .saveAsTableRowJsonFile(dir.getPath)
    }.map(ScioUtil.jsonFactory.toString)
    verifyTap(t, Set(1, 2, 3).map(i => ScioUtil.jsonFactory.toString(newTableRow(i))))
    FileUtils.deleteDirectory(dir)
  }
}
