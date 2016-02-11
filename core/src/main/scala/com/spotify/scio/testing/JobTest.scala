package com.spotify.scio.testing

import java.lang.reflect.InvocationTargetException

import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.util.ScioUtil

import scala.reflect.ClassTag

/**
 * Set up a Dataflow job for unit testing.
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
 *       .output(TextIO("out.txt")) { actual => actual should equalInAnyOrder (expected) }
 *
 *       // Run job test
 *       .run()
 *   }
 * }
 * }}}
 */
object JobTest {

  case class Builder(className: String, cmdlineArgs: Array[String],
                     inputs: Map[TestIO[_], Iterable[_]],
                     outputs: Map[TestIO[_], PCollection[_] => Unit],
                     distCaches: Map[DistCacheIO[_], _]) {

    def args(newArgs: String*): Builder = this.copy(cmdlineArgs = (this.cmdlineArgs.toSeq ++ newArgs).toArray)

    def input[T](key: TestIO[T], value: Iterable[T]): Builder = this.copy(inputs = this.inputs + (key -> value))

    def output[T](key: TestIO[T])(value: PCollection[T] => Unit): Builder =
      this.copy(outputs = this.outputs + (key -> value.asInstanceOf[PCollection[_] => Unit]))

    def distCache[T](key: DistCacheIO[T], value: T): Builder = this.copy(distCaches = this.distCaches + (key -> value))

    def run(): Unit = {
      val testId = className + "-" + System.currentTimeMillis()
      TestDataManager.setInput(testId, new TestInput(inputs))
      TestDataManager.setOutput(testId, new TestOutput(outputs))
      TestDataManager.setDistCache(testId, new TestDistCache(distCaches))

      try {
        Class
          .forName(className)
          .getMethod("main", classOf[Array[String]])
          .invoke(null, cmdlineArgs :+ s"--testId=$testId")
      } catch {
        // InvocationTargetException stacktrace is noisy and useless
        case e: InvocationTargetException => throw e.getCause
        case e: Throwable => throw e
      }

      TestDataManager.unsetInput(testId)
      TestDataManager.unsetOutput(testId)
      TestDataManager.unsetDistCache(testId)
    }

  }

  /** Create a new JobTest.Builder instance. */
  def apply(className: String): Builder = Builder(className, Array(), Map.empty, Map.empty, Map.empty)

  /** Create a new JobTest.Builder instance. */
  def apply[T: ClassTag]: Builder = {
    val className= ScioUtil.classOf[T].getName.replaceAll("\\$$", "")
    Builder(className, Array(), Map.empty, Map.empty, Map.empty)
  }

}
