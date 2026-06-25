/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.parquet

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.read.ParquetReadConfiguration
import com.spotify.scio.parquet.types._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.util.ItUtils
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory

import scala.jdk.CollectionConverters._

case class ParquetITRecord(id: Int, value: String)

class ParquetReadIT extends PipelineSpec {

  private def gcsDir: String = ItUtils.gcpTempLocation("parquet-read-it")

  private def gcpOptions: Array[String] = {
    val tempLocation = ItUtils.gcpTempLocation("parquet-read-it-tmp")
    Array(s"--project=${ItUtils.project}", s"--tempLocation=$tempLocation")
  }

  private def deleteDir(dir: String): Unit = {
    val opts = PipelineOptionsFactory.create()
    opts.as(classOf[GcpOptions]).setProject(ItUtils.project)
    FileSystems.setDefaultPipelineOptions(opts)
    val files = FileSystems.`match`(s"$dir/**").metadata().asScala.map(_.resourceId())
    if (files.nonEmpty) {
      FileSystems.delete(files.asJava)
    }
  }

  "typedParquetFile" should "round-trip through GCS" in {
    val dir = gcsDir
    val records = (1 to 10).map(i => ParquetITRecord(i, s"value-$i")).toList

    try {
      val (sc1, _) = ContextAndArgs(gcpOptions)
      sc1.parallelize(records).saveAsTypedParquetFile(dir)
      sc1.run().waitUntilDone()

      val legacyReadConf = ParquetConfiguration.of(
        ParquetReadConfiguration.UseSplittableDoFn -> false,
        "fs.gs.auth.type" -> "APPLICATION_DEFAULT"
      )

      val (sc2, _) = ContextAndArgs(gcpOptions)
      sc2.typedParquetFile[ParquetITRecord](
        path = s"$dir/*.parquet",
        conf = legacyReadConf
      ) should containInAnyOrder(records)
      sc2.run()
    } finally {
      deleteDir(dir)
    }
  }
}
