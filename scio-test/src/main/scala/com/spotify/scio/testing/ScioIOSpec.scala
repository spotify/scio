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
import com.spotify.scio.coders.Coder
import org.apache.commons.io.FileUtils

import scala.reflect.ClassTag

/** Trait for unit testing [[ScioIO]]. */
trait ScioIOSpec extends PipelineSpec {

  def testTap[T: Coder](xs: Seq[T])(writeFn: (SCollection[T], String) => ClosedTap[T])(
    suffix: String): Unit = {
    val tmpDir = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())

    val sc = ScioContext()
    val data = sc.parallelize(xs)
    val closedTap = writeFn(data, tmpDir.getAbsolutePath)
    val scioResult = sc.close().waitUntilDone()
    val tap = scioResult.tap(closedTap)

    tap.value.toSeq should contain theSameElementsAs xs
    tap.open(ScioContext()) should containInAnyOrder(xs)
    val files =
      tmpDir.listFiles().filterNot(_.getName.startsWith("_")).map(_.toString)
    all(files) should endWith(suffix)
    FileUtils.deleteDirectory(tmpDir)
  }

  def testJobTestInput[T: ClassTag: Coder](xs: Seq[T], in: String = "in")(
    ioFn: String => ScioIO[T])(readFn: (ScioContext, String) => SCollection[T]): Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      readFn(sc, argz("input")).saveAsTextFile("out")
      sc.close()
    }

    val builder = com.spotify.scio.testing
      .JobTest("null")
      .input(ioFn(in), xs)
      .output(TextIO("out"))(_ should containInAnyOrder(xs.map(_.toString)))
    builder.setUp()
    runMain(Array(s"--input=$in") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    // scalastyle:off no.whitespace.before.left.bracket
    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .input(CustomIO[T](in), xs)
        .output(TextIO("out"))(_ should containInAnyOrder(xs.map(_.toString)))
      builder.setUp()
      runMain(Array(s"--input=$in") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test input: ${ioFn(in).testId}, " +
      s"available: [CustomIO($in)]"
    // scalastyle:on no.whitespace.before.left.bracket
  }

  def testJobTestOutput[T: Coder, WT](xs: Seq[T], out: String = "out")(ioFn: String => ScioIO[T])(
    writeFn: (SCollection[T], String) => ClosedTap[WT]): Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      writeFn(sc.parallelize(xs), argz("output"))
      sc.close()
    }

    val builder = com.spotify.scio.testing
      .JobTest("null")
      .output(ioFn(out))(_ should containInAnyOrder(xs))
    builder.setUp()
    runMain(Array(s"--output=$out") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    // scalastyle:off no.whitespace.before.left.bracket
    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .output(CustomIO[T](out))(_ should containInAnyOrder(xs))
      builder.setUp()
      runMain(Array(s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test output: ${ioFn(out).testId}, " +
      s"available: [CustomIO($out)]"
    // scalastyle:on no.whitespace.before.left.bracket
  }

  def testJobTest[T: Coder](xs: Seq[T], in: String = "in", out: String = "out")(
    ioFn: String => ScioIO[T])(readFn: (ScioContext, String) => SCollection[T])(
    writeFn: (SCollection[T], String) => ClosedTap[_]): Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      val data = readFn(sc, argz("input"))
      writeFn(data, argz("output"))
      sc.close()
    }

    val builder = com.spotify.scio.testing
      .JobTest("null")
      .input(ioFn(in), xs)
      .output(ioFn(out))(_ should containInAnyOrder(xs))
    builder.setUp()
    runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    // scalastyle:off no.whitespace.before.left.bracket
    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .input(CustomIO[T](in), xs)
        .output(ioFn(out))(_ should containInAnyOrder(xs))
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test input: ${ioFn(in).testId}, " +
      s"available: [CustomIO($in)]"

    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .input(ioFn(in), xs)
        .output(CustomIO[T](out))(_ should containInAnyOrder(xs))
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test output: ${ioFn(out).testId}, " +
      s"available: [CustomIO($out)]"
    // scalastyle:on no.whitespace.before.left.bracket
  }

}
