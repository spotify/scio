package com.spotify.cloud.dataflow.testing

import com.google.cloud.dataflow.sdk.values.PCollection

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

      Class
        .forName(className)
        .getMethod("main", classOf[Array[String]])
        .invoke(null, cmdlineArgs :+ s"--testId=$testId")

      TestDataManager.unsetInput(testId)
      TestDataManager.unsetOutput(testId)
      TestDataManager.unsetDistCache(testId)
    }

  }

  def apply(className: String): Builder = Builder(className, Array(), Map.empty, Map.empty, Map.empty)

}
