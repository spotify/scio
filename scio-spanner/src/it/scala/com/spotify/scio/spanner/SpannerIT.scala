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

import com.google.cloud.spanner.{Key, KeySet, Mutation, Struct}
import com.spotify.scio._
import com.spotify.scio.testing._

object SpannerIT {

  val projectId = "serious-amp-205423"
  val instanceId = "scio-test"
  val databaseId = "users"
  val table = "users"

  // test data, tuples of user data
  val testData = Seq(
    ("10000", "someFirstName1", "someLastName1", "ACTIVE"),
    ("10001", "someFirstName2", "someLastName2", "INACTIVE")
  )

  def toInsertMutation(user: (String, String, String, String)): Mutation = {

    val (id, first, last, status) = user
    Mutation.newInsertBuilder(table)
      .set("id").to(id)
      .set("firstName").to(first)
      .set("lastName").to(last)
      .set("status").to(status)
      .build
  }

  def toStruct(user: (String, String, String, String)): Struct = {

    val (id, first, last, status) = user
    Struct.newBuilder
      .set("id").to(id)
      .set("firstName").to(first)
      .set("lastName").to(last)
      .set("status").to(status)
      .build
  }

  def toDeleteMutation(id: String): Mutation =
    Mutation.delete(table, Key.newBuilder.append(id).build)
}

class SpannerIT extends PipelineSpec {

  import SpannerIT._

  "SpannerIO" should "work" in {

    try {
      // write data to Spanner
      val sc1 = ScioContext()
      sc1
        .parallelize(testData)
        .map(toInsertMutation)
        .saveAsSpanner(projectId, instanceId, databaseId)
      sc1.close.waitUntilFinish()

      // read data back from Spanner
      val sc2 = ScioContext()

      // only select previously written data
      val keySet = KeySet.newBuilder
        .addKey(Key.newBuilder.append("10000").build)
        .addKey(Key.newBuilder.append("10001").build)
        .build

      sc2
        .spannerFromTable(
          projectId, instanceId, databaseId, table,
          columns = Seq("id", "firstName", "lastName", "status"),
          keySet = keySet
        ) should containInAnyOrder (testData.map(toStruct))
      sc2.close.waitUntilFinish()

    } catch {
      case e: Throwable => throw e

    } finally {
      // delete test data
      val sc = ScioContext()
      sc.parallelize(testData)
        .map(user => toDeleteMutation(user._1))
        .saveAsSpanner(projectId, instanceId, databaseId)
      sc.close.waitUntilFinish()
    }
  }
}
