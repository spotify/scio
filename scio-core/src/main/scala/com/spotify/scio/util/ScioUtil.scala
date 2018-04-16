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

package com.spotify.scio.util

import java.net.URI
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.scio.ScioContext
import org.apache.beam.sdk.coders.{Coder, CoderRegistry}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.Transport
import org.apache.beam.sdk.{PipelineResult, PipelineRunner}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[scio] object ScioUtil {

  @transient lazy private val log = LoggerFactory.getLogger(this.getClass)
  @transient lazy val jsonFactory = Transport.getJsonFactory

  def isLocalUri(uri: URI): Boolean = uri.getScheme == null || uri.getScheme == "file"

  def isRemoteUri(uri: URI): Boolean = !isLocalUri(uri)

  def isLocalRunner(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Boolean = {
    require(runner != null, "Pipeline runner not set!")
    // FIXME: cover Flink, Spark, etc. in local mode
    runner.getName == "org.apache.beam.runners.direct.DirectRunner"
  }

  def isRemoteRunner(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Boolean =
    !isLocalRunner(runner)

  def classOf[T: ClassTag]: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  def getScalaCoder[T: ClassTag]: Coder[T] = {
    import com.spotify.scio.Implicits._

    val coderRegistry = CoderRegistry.createDefault()
    coderRegistry.registerScalaCoders()

    val options = PipelineOptionsFactory.create()
    coderRegistry.getScalaCoder[T](options)
  }

  def getScalaJsonMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def addPartSuffix(path: String, ext: String = ""): String =
    if (path.endsWith("/")) s"${path}part-*$ext" else s"$path/part-*$ext"

  def getTempFile(context: ScioContext, fileOrPath: String = null): String = {
    val fop = Option(fileOrPath).getOrElse("scio-materialize-" + UUID.randomUUID().toString)
    val uri = URI.create(fop)
    if ((ScioUtil.isLocalUri(uri) && uri.toString.startsWith("/")) || uri.isAbsolute) {
      fop
    } else {
      val filename = fop
      val tmpDir = if (context.options.getTempLocation != null) {
        context.options.getTempLocation
      } else {
        val m =
          "Specify a temporary location via --tempLocation or PipelineOptions.setTempLocation."
        Try(context.optionsAs[GcpOptions].getGcpTempLocation) match {
          case Success(l) =>
            log.warn(
              "Using GCP temporary location as a temporary location to materialize data. " + m)
            l
          case Failure(_) =>
            throw new IllegalArgumentException("No temporary location was specified. " + m)
        }
      }
      tmpDir + (if (tmpDir.endsWith("/")) "" else "/") + filename
    }
  }

}
