/*
 * Copyright 2017 Spotify AB.
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

import java.io.File
import java.net.URLClassLoader

import com.spotify.scio.RunnerContext
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions
import org.apache.beam.sdk.options.PipelineOptions
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** Dataflow runner specific context. */
case object DataflowContext extends RunnerContext {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def prepareOptions(options: PipelineOptions,
                              artifacts: List[String]): Unit =
    options
      .as(classOf[DataflowPipelineWorkerPoolOptions])
      .setFilesToStage(getFilesToStage(artifacts).toList.asJava)

  // =======================================================================
  // Extra artifacts - jars/files etc
  // =======================================================================

  /** Compute list of local files to make available to workers. */
  private def getFilesToStage(
    extraLocalArtifacts: List[String]): Iterable[String] = {
    val finalLocalArtifacts = detectClassPathResourcesToStage(
      classOf[DataflowRunner].getClassLoader) ++ extraLocalArtifacts

    logger.debug(
      s"Final list of extra artifacts: ${finalLocalArtifacts.mkString(":")}")
    finalLocalArtifacts
  }

  /** Borrowed from DataflowRunner. */
  private def detectClassPathResourcesToStage(
    classLoader: ClassLoader): Iterable[String] = {
    require(
      classLoader.isInstanceOf[URLClassLoader],
      "Current ClassLoader is '" + classLoader + "' only URLClassLoaders are supported")

    // exclude jars from JAVA_HOME and files from current directory
    val javaHome = new File(sys.props("java.home")).getCanonicalPath
    val userDir = new File(sys.props("user.dir")).getCanonicalPath

    val classPathJars = classLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(url => new File(url.toURI).getCanonicalPath)
      .filter(path => !path.startsWith(javaHome) && path != userDir)

    logger.debug(s"Classpath jars: ${classPathJars.mkString(":")}")

    classPathJars
  }

}
