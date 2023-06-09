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

package com.spotify.scio.testing

import java.io.File
import com.spotify.scio._
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.io.FileBasedSink
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.windowing.{
  BoundedWindow,
  GlobalWindow,
  IntervalWindow,
  PaneInfo
}
import org.apache.commons.io.FileUtils

import java.nio.file.Files
import scala.reflect.ClassTag

/** Trait for unit testing [[ScioIO]]. */
trait ScioIOSpec extends PipelineSpec {
  def testPrefix: String = "foo-shard-"
  def testShardNameTemplate: String = "SS-of-num-shards-NN"

  def testFilenamePolicySupplier(
    path: String,
    suffix: String
  ): FilenamePolicy = {
    val resource = FileBasedSink.convertToFileResourceIfPossible(path)
    new FilenamePolicy {
      override def windowedFilename(
        shardNumber: Int,
        numShards: Int,
        window: BoundedWindow,
        paneInfo: PaneInfo,
        outputFileHints: FileBasedSink.OutputFileHints
      ): ResourceId = {
        val w = window match {
          case iw: IntervalWindow => s"-window${iw.start().getMillis}->${iw.end().getMillis}"
          case _: GlobalWindow    => "-windowglobal"
          case _                  => s"-window${window.maxTimestamp().getMillis}"
        }
        val p = {
          val unitary = paneInfo.isFirst && paneInfo.isLast
          if (unitary) "" else s"-pane${paneInfo.getTiming}-index${paneInfo.getIndex}"
        }
        val filename = s"foo-shard-$shardNumber-of-num-shards-$numShards$w$p"
        resource.getCurrentDirectory.resolve(
          filename + suffix + outputFileHints.getSuggestedFilenameSuffix,
          StandardResolveOptions.RESOLVE_FILE
        )
      }

      override def unwindowedFilename(
        shardNumber: Int,
        numShards: Int,
        outputFileHints: FileBasedSink.OutputFileHints
      ): ResourceId = {
        val filename = s"foo-shard-$shardNumber-of-num-shards-$numShards"
        resource.getCurrentDirectory.resolve(
          filename + suffix + outputFileHints.getSuggestedFilenameSuffix,
          StandardResolveOptions.RESOLVE_FILE
        )
      }
    }
  }

  def listFiles(tmpDir: File): Seq[File] = {
    tmpDir
      .listFiles()
      .filterNot(x => x.getName.startsWith("_"))
      .filterNot(x => x.getName.startsWith("."))
  }

  def testTap[T: Coder](
    xs: Seq[T]
  )(writeFn: (SCollection[T], String) => ClosedTap[T])(suffix: String): Unit = {
    val tmpDir = Files.createTempDirectory("scio-test-").toFile

    val sc = ScioContext()
    val data = sc.parallelize(xs)
    val closedTap = writeFn(data, tmpDir.getAbsolutePath)
    val scioResult = sc.run().waitUntilDone()
    val tap = scioResult.tap(closedTap)

    val files = listFiles(tmpDir).map(_.getName)
    tap.value.toSeq should contain theSameElementsAs xs
    tap.open(ScioContext()) should containInAnyOrder(xs)
    all(files) should endWith(suffix)
    FileUtils.deleteDirectory(tmpDir)
  }

  def testJobTestInput[T: ClassTag: Coder](xs: Seq[T], in: String = "in")(
    ioFn: String => ScioIO[T]
  )(readFn: (ScioContext, String) => SCollection[T]): Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      readFn(sc, argz("input")).saveAsTextFile("out")
      sc.run()
      ()
    }

    val builder = com.spotify.scio.testing
      .JobTest("null")
      .input(ioFn(in), xs)
      .output(TextIO("out")) { coll =>
        coll should containInAnyOrder(xs.map(_.toString))
        ()
      }
    builder.setUp()
    runMain(Array(s"--input=$in") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .input(CustomIO[T](in), xs)
        .output(TextIO("out")) { coll =>
          coll should containInAnyOrder(xs.map(_.toString))
          ()
        }
      builder.setUp()
      runMain(Array(s"--input=$in") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test input: ${ioFn(in).testId}, " +
      s"available: [CustomIO($in)]"
    ()
  }

  def testJobTestOutput[T: Coder, WT](xs: Seq[T], out: String = "out")(
    ioFn: String => ScioIO[T]
  )(writeFn: (SCollection[T], String) => ClosedTap[WT]): Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      writeFn(sc.parallelize(xs), argz("output"))
      sc.run()
      ()
    }

    val builder = com.spotify.scio.testing
      .JobTest("null")
      .output(ioFn(out)) { coll =>
        coll should containInAnyOrder(xs)
        ()
      }
    builder.setUp()
    runMain(Array(s"--output=$out") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .output(CustomIO[T](out)) { coll =>
          coll should containInAnyOrder(xs)
          ()
        }
      builder.setUp()
      runMain(Array(s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test output: ${ioFn(out).testId}, " +
      s"available: [CustomIO($out)]"
    ()
  }

  def testJobTest[T: Coder](xs: Seq[T], in: String = "in", out: String = "out")(
    ioFn: String => ScioIO[T]
  )(
    readFn: (ScioContext, String) => SCollection[T]
  )(writeFn: (SCollection[T], String) => ClosedTap[_]): Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      val data = readFn(sc, argz("input"))
      writeFn(data, argz("output"))
      sc.run()
      ()
    }

    val builder = com.spotify.scio.testing
      .JobTest("null")
      .input(ioFn(in), xs)
      .output(ioFn(out)) { coll =>
        coll should containInAnyOrder(xs)
        ()
      }
    builder.setUp()
    runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .input(CustomIO[T](in), xs)
        .output(ioFn(out)) { coll =>
          coll should containInAnyOrder(xs)
          ()
        }
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test input: ${ioFn(in).testId}, " +
      s"available: [CustomIO($in)]"

    the[IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing
        .JobTest("null")
        .input(ioFn(in), xs)
        .output(CustomIO[T](out)) { coll =>
          coll should containInAnyOrder(xs)
          ()
        }
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test output: ${ioFn(out).testId}, " +
      s"available: [CustomIO($out)]"
    ()
  }
}
