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

package com.spotify.scio.util

import java.io.{File, FileOutputStream}
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.coders.{Coder, CoderRegistry}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryTableRowIterator
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.runners.dataflow.options._
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.services.dataflow.Dataflow
import com.google.api.services.dataflow.model.JobMetrics
import org.apache.beam.sdk.util.{GcsUtil, Transport}
import org.apache.beam.sdk.util.gcsfs.GcsPath
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

private[scio] object ScioUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val jsonFactory = Transport.getJsonFactory

  def isLocalUri(uri: URI): Boolean = uri.getScheme == null || uri.getScheme == "file"

  def isGcsUri(uri: URI): Boolean = uri.getScheme == "gs"

  def classOf[T: ClassTag]: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  def getScalaCoder[T: ClassTag]: Coder[T] = {
    import com.spotify.scio.Implicits._

    val coderRegistry = new CoderRegistry()
    coderRegistry.registerStandardCoders()
    coderRegistry.registerScalaCoders()

    coderRegistry.getScalaCoder[T]
  }

  def isLocalRunner(options: PipelineOptions): Boolean = {
    val runner = options.getRunner
    require(runner != null, "Pipeline runner not set!")
    runner.isAssignableFrom(classOf[DirectRunner])
  }

  def getScalaJsonMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def getDataflowServiceClient(options: PipelineOptions): Dataflow =
    options.as(classOf[DataflowPipelineDebugOptions]).getDataflowClient

  def executeWithBackOff[T](request: AbstractGoogleClientRequest[T], errorMsg: String): T = {
    // Reuse util method from BigQuery
    BigQueryTableRowIterator.executeWithBackOff(request, errorMsg)
  }

  def getDataflowServiceMetrics(options: DataflowPipelineOptions, jobId: String): JobMetrics = {
    val getMetrics = ScioUtil.getDataflowServiceClient(options)
      .projects()
      .jobs()
      .getMetrics(options.getProject, jobId)

    ScioUtil.executeWithBackOff(getMetrics,
      s"Could not get dataflow metrics of ${getMetrics.getJobId} in ${getMetrics.getProjectId}")
  }

  /**
   * Download a file from GCS onto the local file system.
   *
   * There can be multiple DoFns/Threads trying to fetch the same data/files on the same
   * worker. To prevent from situation where some workers see incomplete data, and keep the
   * solution simple - let's make this method synchronized. There is a downside - more
   * specifically we might have to wait a bit longer then in more optimal solution, but,
   * simplicity > performance.
   */
  def fetchFromGCS(gcsUtil: GcsUtil, gcsUri: URI, dest: String): File = synchronized {
    require(isGcsUri(gcsUri), "Invalid GCS URI.")
    val file = new File(dest)
    val srcPath = GcsPath.fromUri(gcsUri)
    val srcSize = gcsUtil.fileSize(srcPath)

    if (file.exists() && file.length() != srcSize) {
      logger.warn("Existing destination file with wrong size. " +
        s"Source = $srcSize, destination = ${file.length()}")
      // File exists but has different size than source file, most likely there was an issue
      // on previous thread, let's remove invalid file, and download it again.
      file.delete()
    }

    if (!file.exists()) {
      val fos = new FileOutputStream(dest)
      val dst = fos.getChannel
      val src = gcsUtil.open(srcPath)
      val size = dst.transferFrom(src, 0, src.size())
      dst.close()
      fos.close()
      logger.info(s"GCS URI $gcsUri to $dest, copied $size bytes")
    } else {
      logger.info(s"GCS URI $gcsUri already downloaded")
    }

    file
  }

}
