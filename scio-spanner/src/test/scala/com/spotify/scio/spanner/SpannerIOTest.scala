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
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig
import org.scalatest.Matchers

object QueryReadJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val out = args("out")

    sc.spannerQuery(SpannerIOTest.spannerConfig, s"Select * From someTable")
      .saveAsTextFile(out)

    sc.close()
  }
}

object TableReadJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val out = args("out")

    sc.spannerTable(SpannerIOTest.spannerConfig, "someTable", Seq("someColumn"))
      .saveAsTextFile(out)

    sc.close()
  }
}

object SpannerWriteJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(
        Seq(
          Mutation.newInsertBuilder("someTable").set("foo").to("bar").build()
        ))
      .saveAsSpanner(SpannerIOTest.spannerConfig)

    sc.close()
  }
}

object SpannerIOTest {
  val spannerConfig: SpannerConfig = SpannerConfig
    .create()
    .withProjectId("someProject")
    .withDatabaseId("someDatabase")
    .withInstanceId("someInstance")
}

class SpannerIOTest extends PipelineSpec with Matchers {
  import SpannerIOTest.spannerConfig

  "SpannerScioContext" should "support reading from query" in {
    val out = "someOutput"
    val spannerRows = Seq(Struct.newBuilder().set("foo").to("bar").build())

    JobTest[QueryReadJob.type]
      .args(s"--out=$out")
      .input[Struct](SpannerRead(spannerConfig), spannerRows)
      .output[String](TextIO(out))(_ should containInAnyOrder(spannerRows.map(_.toString)))
      .run()
  }

  it should "support reading from table" in {
    val out = "someOutput"
    val spannerRows = Seq(Struct.newBuilder().set("foo").to("bar").build())

    JobTest[TableReadJob.type]
      .args(s"--out=$out")
      .input[Struct](SpannerRead(spannerConfig), spannerRows)
      .output[String](TextIO(out))(_ should containInAnyOrder(spannerRows.map(_.toString)))
      .run()
  }

  "SpannerSCollection" should "support writes" in {
    val spannerRows = Seq(Mutation.newInsertBuilder("someTable").set("foo").to("bar").build())

    JobTest[SpannerWriteJob.type]
      .output[Mutation](SpannerWrite(SpannerIOTest.spannerConfig))(_ should
        containInAnyOrder(spannerRows))
      .run()
  }
}
