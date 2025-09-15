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

package com.spotify.scio.runners.dataflow

import com.spotify.scio.RunnerContext
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.{
  DataflowPipelineOptions,
  DataflowPipelineWorkerPoolOptions
}
import org.apache.beam.sdk.options.PipelineOptions

import scala.jdk.CollectionConverters._

/** Dataflow runner specific context. */
case object DataflowContext extends RunnerContext {

  private lazy val JavaMajorVersion: Int =
    System.getProperty("java.version").stripPrefix("1.").takeWhile(_.isDigit).toInt

  override def prepareOptions(options: PipelineOptions, artifacts: List[String]): Unit = {
    val classLoader = classOf[DataflowRunner].getClassLoader
    val dataflowOptions = options.as(classOf[DataflowPipelineWorkerPoolOptions])
    val localArtifacts = dataflowOptions.getFilesToStage() match {
      case null => Nil
      case l    => l.asScala
    }
    val filesToStage = RunnerContext
      .filesToStage(options, classLoader, localArtifacts, artifacts)
      .asJavaCollection

    // Required for Kryo w/ Java 17+
    lazy val dataflowPipelineOpts = options.as(classOf[DataflowPipelineOptions])
    if (JavaMajorVersion >= 17 && dataflowPipelineOpts.getJdkAddOpenModules == null) {
      dataflowPipelineOpts.setJdkAddOpenModules(
        List(
          "java.base/java.io=ALL-UNNAMED",
          "java.base/java.lang=ALL-UNNAMED",
          "java.base/java.lang.invoke=ALL-UNNAMED",
          "java.base/java.lang.reflect=ALL-UNNAMED",
          "java.base/java.net=ALL-UNNAMED",
          "java.base/java.nio=ALL-UNNAMED",
          "java.base/java.text=ALL-UNNAMED",
          "java.base/java.time=ALL-UNNAMED",
          "java.base/java.util=ALL-UNNAMED",
          "java.base/java.util.concurrent=ALL-UNNAMED",
          "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
          "java.base/java.util.concurrent.locks=ALL-UNNAMED"
        ).asJava
      )
    }

    dataflowOptions.setFilesToStage(new java.util.ArrayList(filesToStage))
  }

}
