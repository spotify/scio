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

package com.spotify.scio.spanner

import com.google.cloud.spanner.{Mutation, Struct}
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.testing.ScioIOSpec
import org.apache.beam.sdk.io.gcp.spanner.{ReadOperation, SpannerConfig}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.{CoderUtils, SerializableUtils}
import org.scalactic.Equality
import org.scalatest.{Assertion, Matchers}

class SpannerIOTest extends ScioIOSpec with Matchers {
  private val config: SpannerConfig = SpannerConfig
    .create()
    .withProjectId("someProject")
    .withDatabaseId("someDatabase")
    .withInstanceId("someInstance")

  private val readData = Seq(Struct.newBuilder().set("foo").to("bar").build())
  private val writeData = Seq(
    Mutation.newInsertBuilder("someTable").set("foo").to("bar").build()
  )

  private def checkCoder[T](t: T)(implicit C: Coder[T], eq: Equality[T]): Assertion = {
    val options = PipelineOptionsFactory.create()
    val beamCoder = CoderMaterializer.beamWithDefault(C, o = options)
    SerializableUtils.ensureSerializable(beamCoder)
    val enc = CoderUtils.encodeToByteArray(beamCoder, t)
    val dec = CoderUtils.decodeFromByteArray(beamCoder, enc)
    dec should ===(t)
  }

  "SpannerScioContext" should "support table input" in {
    testJobTestInput(readData)(_ => SpannerRead(config))(
      _.spannerTable(config, _, Seq("someColumn")))
  }

  it should "support query input" in {
    testJobTestInput(readData)(_ => SpannerRead(config))(_.spannerQuery(config, _))
  }

  "SpannerSCollection" should "support writes" in {
    testJobTestOutput(writeData)(_ => SpannerWrite(config))((data, _) => data.saveAsSpanner(config))
  }

  "Spanner coders" should "#1447: Properly serde spanner's ReadOperation" in {
    val ro = ReadOperation.create().withQuery("SELECT 1")
    checkCoder(ro)
  }

  it should "support spanner's Mutation class" in {
    checkCoder(Mutation.newInsertBuilder("myTable").set("foo").to("bar").build())
  }

  it should "support spanner's Struct class" in {
    checkCoder(Struct.newBuilder().set("foo").to("bar").build())
  }
}
