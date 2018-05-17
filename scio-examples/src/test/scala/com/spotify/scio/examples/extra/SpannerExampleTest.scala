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

package com.spotify.scio.examples.extra

import com.google.cloud.spanner.{Mutation, Struct}
import com.spotify.scio.testing.{CustomIO, PipelineSpec}
import org.apache.beam.sdk.io.gcp.spanner.{MutationGroup, SpannerConfig}

class SpannerExampleTest extends PipelineSpec {

  val projectId = "project-id"
  val instanceId = "spanner-instance-id"
  val databaseId = "spanner-database-id"

  val config = SpannerConfig.create
    .withProjectId(projectId)
    .withInstanceId(instanceId)
    .withDatabaseId(databaseId)

  val cmdlineArgs = Seq(
    s"--spannerProjectId=$projectId",
    s"--spannerInstanceId=$instanceId",
    s"--spannerDatabaseId=$databaseId"
  )

  val spannerInput = CustomIO[Struct](config.toString)
  val structs = Seq(
    Struct.newBuilder
      .set("id").to("10000")
      .set("firstName").to("someFirstName1")
      .set("lastName").to("someLastName1")
      .set("status").to("ACTIVE")
      .build,
    Struct.newBuilder
      .set("id").to("10001")
      .set("firstName").to("someFirstName2")
      .set("lastName").to("someLastName2")
      .set("status").to("INACTIVE")
      .build
  )

  "SpannerReadFromTableExample" should "work" in {

    val spannerOutput = CustomIO[Mutation](config.toString)
    val expectedMutations = Seq(
      Mutation.newInsertBuilder("usersBackup")
        .set("id").to("10000")
        .set("firstName").to("someFirstName1")
        .set("lastName").to("someLastName1")
        .build,
      Mutation.newInsertBuilder("usersBackup")
        .set("id").to("10001")
        .set("firstName").to("someFirstName2")
        .set("lastName").to("someLastName2")
        .build
    )

    JobTest[com.spotify.scio.examples.extra.SpannerReadFromTableExample.type]
      .args(cmdlineArgs: _*)
      .input(spannerInput, structs)
      .output(spannerOutput) { _ should containInAnyOrder(expectedMutations) }
      .run
  }

  "SpannerReadFromQueryExample" should "work" in {

    val spannerOutput = CustomIO[MutationGroup](config.toString)
    val expectedMutationGroups = Seq(
      MutationGroup.create(
        Mutation.newInsertBuilder("usersBackup")
          .set("id").to("10000")
          .set("firstName").to("someFirstName1")
          .set("lastName").to("someLastName1")
          .build,
        Mutation.newInsertBuilder("usersActive")
          .set("id").to("10000")
          .build
      ),
      MutationGroup.create(
        Mutation.newInsertBuilder("usersBackup")
          .set("id").to("10001")
          .set("firstName").to("someFirstName2")
          .set("lastName").to("someLastName2")
          .build
      )
    )

    JobTest[com.spotify.scio.examples.extra.SpannerReadFromQueryExample.type]
      .args(cmdlineArgs: _*)
      .input(spannerInput, structs)
      .output(spannerOutput) { _ should containInAnyOrder(expectedMutationGroups) }
      .run
  }
}
