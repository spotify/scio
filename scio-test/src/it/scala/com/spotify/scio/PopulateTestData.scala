/*
 * Copyright 2022 Spotify AB.
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

package com.spotify.scio

import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.MimeTypes
import org.slf4j.LoggerFactory

import java.nio.channels.Channels
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

object PopulateTestData {
  private lazy val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = populate("data-integration-test-eu")

  def populate(bucket: String): Unit = {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create())

    val root = Paths.get("src/it/resources")
    Files
      .walk(root)
      .collect(Collectors.toList[Path])
      .asScala
      .filter(Files.isRegularFile(_))
      .foreach { src =>
        val resourceId =
          FileSystems.matchNewResource(s"gs://$bucket/${root.relativize(src)}", false)
        val dst = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.BINARY))
        Files.copy(src, dst)
        dst.close()
        log.info(s"Populated file $resourceId.")
      }
  }
}
