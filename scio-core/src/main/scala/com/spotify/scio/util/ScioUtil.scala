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

package com.spotify.scio.util

import java.net.URI
import java.util.UUID
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.extensions.gcp.util.Transport
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.{DefaultFilenamePolicy, DynamicFileDestinations, FileBasedSink, FileSystems}
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.{PipelineOptions, ValueProvider}
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.values.WindowingStrategy
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}
import org.slf4j.LoggerFactory

import scala.collection.compat.immutable.ArraySeq
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[scio] object ScioUtil {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)
  @transient lazy val jsonFactory = Transport.getJsonFactory

  def isLocalUri(uri: URI): Boolean =
    uri.getScheme == null || uri.getScheme == "file"

  def isRemoteUri(uri: URI): Boolean = !isLocalUri(uri)

  def isLocalRunner(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Boolean = {
    require(runner != null, "Pipeline runner not set!")
    // FIXME: cover Flink, Spark, etc. in local mode
    runner.getName == "org.apache.beam.runners.direct.DirectRunner"
  }

  def isRemoteRunner(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Boolean =
    !isLocalRunner(runner)

  def classOf[T: ClassTag]: Class[T] =
    implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  def getScalaJsonMapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)

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
              "Using GCP temporary location as a temporary location to materialize data. " + m
            )
            l
          case Failure(_) =>
            throw new IllegalArgumentException("No temporary location was specified. " + m)
        }
      }
      tmpDir + (if (tmpDir.endsWith("/")) "" else "/") + filename
    }
  }

  def pathWithPrefix(path: String, prefix: String = null): String = {
    val replacement = s"/${Option(prefix).getOrElse("part")}"
    path.replaceAll("\\/*$", replacement)
  }

  def consistentHashCode[K](k: K): Int = k match {
    case key: Array[_] => ArraySeq.unsafeWrapArray(key).##
    case key           => key.##
  }

  def toResourceId(directory: String): ResourceId =
    FileSystems.matchNewResource(directory, true)

  trait FilenamePolicyCreator {
    // TODO remove isWindowed?
    def apply(path: String, suffix: String, isWindowed: Boolean): FilenamePolicy
  }

  def defaultFilenamePolicy(path: String, shardTemplate: String, suffix: String, isWindowed: Boolean): FilenamePolicy = {
    val resource = FileBasedSink.convertToFileResourceIfPossible(path)
    val prefix = StaticValueProvider.of(resource)
    DefaultFilenamePolicy.fromStandardParameters(prefix, shardTemplate, suffix, isWindowed)
  }

  def filenamePolicyCreatorOf(
    windowed: (Int, Int, BoundedWindow, PaneInfo) => String = null,
    unwindowed: (Int, Int) => String = null
  ): FilenamePolicyCreator = {
    (path: String, suffix: String, isWindowed: Boolean) =>
      new FilenamePolicy {
        val resource = FileBasedSink.convertToFileResourceIfPossible(ScioUtil.pathWithPrefix(path, ""))
        private def resolve(filename: String, outputFileHints: FileBasedSink.OutputFileHints) = {
          resource.getCurrentDirectory.resolve(
            filename + suffix + outputFileHints.getSuggestedFilenameSuffix,
            StandardResolveOptions.RESOLVE_FILE
          )
        }
        override def windowedFilename(shardNumber: Int, numShards: Int, window: BoundedWindow, paneInfo: PaneInfo, outputFileHints: FileBasedSink.OutputFileHints): ResourceId = {
          if(windowed == null) throw new NotImplementedError()
          resolve(windowed(shardNumber, numShards, window, paneInfo), outputFileHints)
        }
        override def unwindowedFilename(shardNumber: Int, numShards: Int, outputFileHints: FileBasedSink.OutputFileHints): ResourceId = {
          if(unwindowed == null) throw new NotImplementedError()
          resolve(unwindowed(shardNumber, numShards), outputFileHints)
        }
      }
  }

  def tempDirOrDefault(tempDirectory: String, sc: ScioContext): ResourceId = {
    Option(tempDirectory).map(toResourceId).getOrElse {
      val tempLocationOpt: String = sc.options.getTempLocation
      FileSystems.matchNewResource(tempLocationOpt, true)
    }
  }

  def isWindowed(coll: SCollection[_]): Boolean = coll.internal.getWindowingStrategy != WindowingStrategy.globalDefault()
}
