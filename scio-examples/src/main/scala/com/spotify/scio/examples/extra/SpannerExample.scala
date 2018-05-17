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
import com.spotify.scio._
import com.spotify.scio.spanner._
import org.apache.beam.sdk.io.gcp.spanner.{MutationGroup, SpannerConfig}

/**
 * Read from Spanner table and commit `Mutation`s back to Spanner.
 *
 * Get all rows from `users` table and write `id`, `firstName`, and
 * `lastName` column values to `usersBackup` table. The `usersBackup`
 * table contains identical column names as the `users` table.
 *
 * Usage:
 *
 * sbt runMain "com.spotify.scio.examples.extra.SpannerReadFromTableExample
 * --project=[PROJECT_ID] --runner=DataflowRunner
 * --spannerProjectId=[SPANNER_PROJECT_ID]
 * --spannerInstanceId=[SPANNER_INSTANCE_ID]
 * --spannerDatabaseId=[SPANNER_DATABASE_ID]"
 */
object SpannerReadFromTableExample {

  def structToMutation(struct: Struct): Mutation = {

    val id = struct.getString("id")
    val firstName = struct.getString("firstName")
    val lastName = struct.getString("lastName")

    val usersBackupTable = "usersBackup"

    Mutation.newInsertBuilder(usersBackupTable)
      .set("id").to(id)
      .set("firstName").to(firstName)
      .set("lastName").to(lastName)
      .build
  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val projectId = args("spannerProjectId")
    val instanceId = args("spannerInstanceId")
    val databaseId = args("spannerDatabaseId")

    val table = "users"
    val columns = Seq("id", "firstName", "lastName")

    sc.spannerFromTable(projectId, instanceId, databaseId, table, columns)
      .map(structToMutation)
      .saveAsSpanner(projectId, instanceId, databaseId)

    sc.close
  }
}

/**
 * Read from Spanner with query and commit `MutationGroup`s back to Spanner.
 *
 * Get all rows from `users` table. Write `id`, `firstName`, and
 * `lastName` column values to `usersBackup` table. Write `id` column
 * value to `usersActive` table if `status` column value is equal to
 * "ACTIVE". Both write operations are done atomically.
 *
 * Usage:
 *
 * sbt runMain "com.spotify.scio.examples.extra.SpannerReadFromQueryExample
 * --project=[PROJECT_ID] --runner=DataflowRunner
 * --spannerProjectId=[SPANNER_PROJECT_ID]
 * --spannerInstanceId=[SPANNER_INSTANCE_ID]
 * --spannerDatabaseId=[SPANNER_DATABASE_ID]"
 */
object SpannerReadFromQueryExample {

  def structToMutationGroup(struct: Struct): MutationGroup = {

    val id = struct.getString("id")
    val firstName = struct.getString("firstName")
    val lastName = struct.getString("lastName")
    val status = struct.getString("status")

    val usersBackupTable = "usersBackup"
    val mutation1 = Mutation.newInsertBuilder(usersBackupTable)
      .set("id").to(id)
      .set("firstName").to(firstName)
      .set("lastName").to(lastName)
      .build

    if (status == "ACTIVE") {
      val usersActiveTable = "usersActive"
      val mutation2 = Mutation.newInsertBuilder(usersActiveTable)
        .set("id").to(id)
        .build

      MutationGroup.create(mutation1, mutation2)
    } else {
      MutationGroup.create(mutation1)
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val projectId = args("spannerProjectId")
    val instanceId = args("spannerInstanceId")
    val databaseId = args("spannerDatabaseId")

    val config = SpannerConfig.create
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .withDatabaseId(databaseId)

    val query = "SELECT id, firstName, lastName, status FROM users"

    sc.spannerFromQueryWithConfig(config, query)
      .map(structToMutationGroup)
      .saveAsSpannerWithConfig(config)

    sc.close
  }
}
