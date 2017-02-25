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

  private[scio] def newTestId(className: String = "TestClass"): String = {
    val uuid = UUID.randomUUID().toString.replaceAll("-", "")
    s"JobTest-$className-$uuid"
  }

  private[scio] def isTestId(appName: String): Boolean =
    "JobTest-[^-]+-[a-z0-9]+".r.pattern.matcher(appName).matches()

  case class Builder(className: String, cmdlineArgs: Array[String],
                     inputs: Map[TestIO[_], Iterable[_]],
                     outputs: Map[TestIO[_], SCollection[_] => Unit],
                     distCaches: Map[DistCacheIO[_], _]) {

    /** Test ID for input and output wiring. */
    val testId: String = newTestId(className)

    /** Feed command line arguments to the pipeline being tested. */
    def args(newArgs: String*): Builder =
      this.copy(cmdlineArgs = (this.cmdlineArgs.toSeq ++ newArgs).toArray)

    /**
     * Feed an input to the pipeline being tested. Note that `TestIO[T]` must match the one used
     * inside the pipeline, e.g. `AvroIO[MyRecord]("in.avro")` with
     * `sc.avroFile[MyRecord]("in.avro")`.
     */
    def input[T](key: TestIO[T], value: Iterable[T]): Builder = {
      require(!this.inputs.contains(key), "Duplicate test input: " + key)
      this.copy(inputs = this.inputs + (key -> value))
    }

    /**
     * Evaluate an output of the pipeline being tested. Note that `TestIO[T]` must match the one
     * used inside the pipeline, e.g. `AvroIO[MyRecord]("out.avro")` with
     * `data.saveAsAvroFile("out.avro")` where `data` is of type `SCollection[MyRecord]`.
     */
    def output[T](key: TestIO[T])(value: SCollection[T] => Unit): Builder = {
      require(!this.outputs.contains(key), "Duplicate test output: " + key)
      this.copy(outputs = this.outputs + (key -> value.asInstanceOf[SCollection[_] => Unit]))
    }

    /**
     * Feed an distributed cache to the pipeline being tested. Note that `DistCacheIO[T]` must
     * match the one used inside the pipeline, e.g. `DistCacheIO[Set[String]]("dc.txt")` with
     * `sc.distCache("dc.txt")(f => scala.io.Source.fromFile(f).getLines().toSet)`.
     */
    def distCache[T](key: DistCacheIO[T], value: T): Builder = {
      require(!this.distCaches.contains(key), "Duplicate test dist cache: " + key)
      this.copy(distCaches = this.distCaches + (key -> value))
    }

    /**
     * Set up test wiring. Use this only if you have custom pipeline wiring and are bypassing
     * [[run]]. Make sure [[tearDown]] is called afterwards.
     */
    def setUp(): Unit = {
      TestDataManager.setInput(testId, new TestInput(inputs))
      TestDataManager.setOutput(testId, new TestOutput(outputs))
      TestDataManager.setDistCache(testId, new TestDistCache(distCaches))
    }

    /**
     * Tear down test wiring. Use this only if you have custom pipeline wiring and are bypassing
     * [[run]]. Make sure [[setUp]] is called before.
     */
    def tearDown(): Unit = {
      TestDataManager.unsetInput(testId)
      TestDataManager.unsetOutput(testId)
      TestDataManager.unsetDistCache(testId)
    }

    /** Run the pipeline with test wiring. */
    def run(): Unit = {
      setUp()

      try {
        Class
          .forName(className)
          .getMethod("main", classOf[Array[String]])
          .invoke(null, cmdlineArgs :+ s"--appName=$testId")
      } catch {
        // InvocationTargetException stacktrace is noisy and useless
        case e: InvocationTargetException => throw e.getCause
        case NonFatal(e) => throw e
      }

      tearDown()
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
