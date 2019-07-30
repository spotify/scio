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

package com.spotify.scio.runners.spark

import com.spotify.scio.RunnerContext
import org.apache.beam.runners.spark.{SparkPipelineOptions, SparkRunner}
import org.apache.beam.sdk.options.PipelineOptions

import scala.collection.JavaConverters._

/** Spark runner specific context. */
case object SparkContext extends RunnerContext {
  override def prepareOptions(options: PipelineOptions, artifacts: List[String]): Unit = {
    val classLoader = classOf[SparkRunner].getClassLoader
    val filesToStage = RunnerContext.filesToStage(classLoader, artifacts)

    options
      .as(classOf[SparkPipelineOptions])
      .setFilesToStage(filesToStage.toList.asJava)
  }
}
