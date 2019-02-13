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

package com.spotify.scio.tensorflow

import java.util.UUID

import com.google.protobuf.ByteString
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.util.ItUtils
import com.spotify.scio.testing.{PipelineSpec, PipelineTestUtils}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory
import org.apache.beam.sdk.util.gcsfs.GcsPath
import org.scalatest.BeforeAndAfterAll
import org.tensorflow.example._
import org.tensorflow.example.Feature
import org.tensorflow.metadata.v0.{FeaturePresence, FeatureType, Schema, ValueCount}

import scala.collection.JavaConverters._

object TensorFlowImplicitsIT {
  val tfSchema: Schema = Schema
    .newBuilder()
    .addFeature(
      org.tensorflow.metadata.v0.Feature
        .newBuilder()
        .setName("values")
        .setType(FeatureType.BYTES)
        .setValueCount(ValueCount.newBuilder().setMin(2).setMax(3))
        .setPresence(FeaturePresence.newBuilder().setMinCount(2).setMinFraction(1.0)))
    .build()

  private val examples: Seq[Example] = Seq(
    Map("values" -> byteStrFeature(Seq("one", "nine").map(ByteString.copyFromUtf8))),
    Map("values" -> byteStrFeature(Seq("three", "five", "nine").map(ByteString.copyFromUtf8)))
  ).map { features =>
    Example
      .newBuilder()
      .setFeatures(Features.newBuilder().putAllFeature(features.asJava))
      .build
  }

  private def byteStrFeature(raw: Seq[ByteString]): Feature = {
    val fb = Feature.newBuilder()
    val vals = BytesList.newBuilder()
    raw.foreach(vals.addValue)
    fb.setBytesList(vals)
    fb.build
  }
}

class TensorFlowImplicitsIT extends PipelineSpec with PipelineTestUtils with BeforeAndAfterAll {
  import TensorFlowImplicitsIT._

  private val options = PipelineOptionsFactory.create()
  options.as(classOf[GcpOptions]).setProject(ItUtils.project)

  private val outputPrefix = "gs://data-integration-test-eu/tensorflowIT"

  override def afterAll(): Unit = {
    val gcsUtil = new GcsUtilFactory().create(options)
    val files = gcsUtil.expand(GcsPath.fromUri(s"$outputPrefix/*/*")).asScala.map(_.toString)
    gcsUtil.remove(files.asJava)
  }

  "Storing and loading a Schema file remotely" should "work" in {
    val outputPath = s"$outputPrefix/${UUID.randomUUID}"
    val sc1 = ScioContext(options)

    val closedTap = sc1
      .parallelize(examples)
      .saveAsTfExampleFileWithSchema(path = outputPath,
                                     schema = tfSchema,
                                     schemaFilename = "schema_it.pb",
                                     suffix = ".tfrecords",
                                     compression = Compression.UNCOMPRESSED,
                                     numShards = 0)

    val tap = sc1.close().waitUntilDone().tap(closedTap)

    val sc2 = ScioContext(options)
    val (data, schemaCache) = sc2.tfRecordExampleFileWithSchema(
      path = s"$outputPath/*.tfrecords",
      schemaFilename = s"$outputPath/schema_it.pb",
      compression = Compression.UNCOMPRESSED
    )

    data should containInAnyOrder(tap.value.toSeq)
    sc2.close().waitUntilDone()
    schemaCache() shouldEqual tfSchema
  }
}
