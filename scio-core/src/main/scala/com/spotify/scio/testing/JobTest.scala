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

package com.spotify.scio.testing

import java.lang.reflect.InvocationTargetException
import java.util.UUID

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * Set up a Scio job for end-to-end unit testing.
 * To be used in a [[com.spotify.scio.testing.PipelineSpec PipelineSpec]]. For example:
 *
 * {{{
 * import com.spotify.scio.testing._
 *
 * class WordCountTest extends PipelineSpec {
 *
 *   // Mock input data, mock distributed cache and expected result
 *   val inData = Seq("a b c d e", "a b a b")
 *   val distCache = Map(1 -> "Jan", 2 -> "Feb", 3 -> "Mar")
 *   val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")
 *
 *   // Test specification
 *   "WordCount" should "work" in {
 *     JobTest("com.spotify.scio.examples.WordCount")
 *     // Or the type safe version
 *     // JobTest[com.spotify.scio.examples.WordCount.type]
 *
 *       // Command line arguments
 *       .args("--input=in.txt", "--output=out.txt")
 *
 *       // Mock input data
 *       .input(TextIO("in.txt"), inData)
 *
 *       // Mock distributed cache
 *       .distCache(DistCacheIO("gs://dataflow-samples/samples/misc/months.txt", distCache)
 *
 *       // Verify output
 *       .output(TextIO("out.txt")) { actual => actual should containInAnyOrder (expected) }
 *
 *       // Run job test
 *       .run()
 *   }
 * }
 * }}}
 */
object JobTest {

  def newTestId(className: String = "TestClass"): String = {
    val uuid = UUID.randomUUID().toString.replaceAll("-", "")
    s"JobTest-$className-$uuid"
  }

  def isTestId(appName: String): Boolean =
    "JobTest-[^-]+-[a-z0-9]+".r.pattern.matcher(appName).matches()

  case class Builder(className: String, cmdlineArgs: Array[String],
                     inputs: Map[TestIO[_], Iterable[_]],
                     outputs: Map[TestIO[_], SCollection[_] => Unit],
                     distCaches: Map[DistCacheIO[_], _]) {

    def args(newArgs: String*): Builder =
      this.copy(cmdlineArgs = (this.cmdlineArgs.toSeq ++ newArgs).toArray)

    def input[T](key: TestIO[T], value: Iterable[T]): Builder =
      this.copy(inputs = this.inputs + (key -> value))

    def output[T](key: TestIO[T])(value: SCollection[T] => Unit): Builder =
      this.copy(outputs = this.outputs + (key -> value.asInstanceOf[SCollection[_] => Unit]))

    def distCache[T](key: DistCacheIO[T], value: T): Builder =
      this.copy(distCaches = this.distCaches + (key -> value))

    def run(): Unit = {
      val testId = newTestId(className)
      TestDataManager.setInput(testId, new TestInput(inputs))
      TestDataManager.setOutput(testId, new TestOutput(outputs))
      TestDataManager.setDistCache(testId, new TestDistCache(distCaches))

      try {
        Class
          .forName(className)
          .getMethod("main", classOf[Array[String]])
          .invoke(null, cmdlineArgs :+ s"--appName=$testId"
            :+ s"--runner=${classOf[InProcessPipelineRunner].getSimpleName}")
      } catch {
        // InvocationTargetException stacktrace is noisy and useless
        case e: InvocationTargetException => throw e.getCause
        case NonFatal(e) => throw e
      }

      TestDataManager.unsetInput(testId)
      TestDataManager.unsetOutput(testId)
      TestDataManager.unsetDistCache(testId)
    }

  }

  /** Create a new JobTest.Builder instance. */
  def apply(className: String): Builder =
    Builder(className, Array(), Map.empty, Map.empty, Map.empty)

  /** Create a new JobTest.Builder instance. */
  def apply[T: ClassTag]: Builder = {
    val className= ScioUtil.classOf[T].getName.replaceAll("\\$$", "")
    Builder(className, Array(), Map.empty, Map.empty, Map.empty)
  }

}
