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

package com.spotify.scio.testing

import java.io.File
import java.util.UUID

import com.spotify.scio._
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.commons.io.FileUtils

import scala.concurrent.Future
import scala.reflect.ClassTag

/** Trait for unit testing [[ScioIO]]. */
trait ScioIOSpec extends PipelineSpec {

  def testTap[T: ClassTag](xs: Seq[T])
                          (ioFn: String => ScioIO[T])
                          (readFn: (ScioContext, String) => SCollection[T])
                          (writeFn: (SCollection[T], String) => Future[Tap[T]])
                          (suffix: String): Unit = {
    val tmpDir = new File(
      new File(sys.props("java.io.tmpdir")),
      "scio-test-" + UUID.randomUUID())

    val sc = ScioContext()
    val data = sc.parallelize(xs)
    val future = writeFn(data, tmpDir.getAbsolutePath)
    sc.close().waitUntilDone()
    val tap = future.waitForResult()

    tap.value.toSeq should contain theSameElementsAs xs
    tap.open(ScioContext()) should containInAnyOrder(xs)
    all(tmpDir.listFiles().map(_.toString)) should endWith (suffix)
    FileUtils.deleteDirectory(tmpDir)
  }

  def testJobTest[T: ClassTag](xs: Seq[T], in: String = "in", out: String = "out")
                              (ioFn: String => ScioIO[T])
                              (readFn: (ScioContext, String) => SCollection[T])
                              (writeFn: (SCollection[T], String) => Future[Tap[T]])
  : Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      val data = readFn(sc, argz("input"))
      writeFn(data, argz("output"))
      sc.close()
    }

    val builder = com.spotify.scio.testing.JobTest("null")
      .input(ioFn(in), xs)
      .output(ioFn(out))(_ should containInAnyOrder (xs))
    builder.setUp()
    runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    // scalastyle:off no.whitespace.before.left.bracket
    the [IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing.JobTest("null")
        .input(CustomIO[T](in), xs)
        .output(ioFn(out))(_ should containInAnyOrder (xs))
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test input: ${ioFn(in).testId}, " +
      s"available: [CustomIO($in)]"

    the [IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing.JobTest("null")
        .input(ioFn(in), xs)
        .output(CustomIO[T](out))(_ should containInAnyOrder (xs))
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test output: ${ioFn(out).testId}, " +
      s"available: [CustomIO($out)]"
    // scalastyle:on no.whitespace.before.left.bracket
  }

}
