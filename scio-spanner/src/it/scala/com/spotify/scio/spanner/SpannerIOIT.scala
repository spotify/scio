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

import com.google.cloud.spanner._
import com.spotify.scio.ScioContext
import com.spotify.scio.spanner.client.Spanner
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.Random

object SpannerIOIT {
  private val tablePrefix = "test_table"
  private val projectId = "data-integration-test"
  private val config: SpannerConfig = SpannerConfig.create()
    .withProjectId(projectId)
    .withDatabaseId(s"io_it_${Random.nextInt}")
    .withInstanceId("spanner-it")

  private val adminClient = Spanner.getAdminClient(projectId)
  private val dbClient = Spanner.getDatabaseClient(config)

  private val options = PipelineOptionsFactory.fromArgs(s"--project=$projectId").create()
}

class SpannerIOIT extends FlatSpec with Matchers with BeforeAndAfterAll {
  import SpannerIOIT._
  implicit val ec: ExecutionContext = ExecutionContext.global

  override def beforeAll(): Unit = {
    val create = adminClient.createDatabase(
      config.getInstanceId.get(),
      config.getDatabaseId.get(),
      List(
        s"CREATE TABLE ${tablePrefix}_1 (\n Key INT64, \n Value STRING(MAX) \n) PRIMARY KEY (Key)",
        s"CREATE TABLE ${tablePrefix}_2 (\n Key INT64, \n Value STRING(MAX) \n) PRIMARY KEY (Key)",
        s"CREATE TABLE ${tablePrefix}_3 (\n Key INT64, \n Value STRING(MAX) \n) PRIMARY KEY (Key)"
      ).asJava)

    create.waitFor()
  }

  override def afterAll(): Unit = {
    adminClient.dropDatabase(config.getInstanceId.get(), config.getDatabaseId.get())
  }

  "SpannerIO" should "perform writes" in {
    val table = s"${tablePrefix}_1"
    val mutations = Seq(
      Mutation.newInsertBuilder(table).set("Key").to(1L).set("Value").to("foo").build(),
      Mutation.newInsertBuilder(table).set("Key").to(2L).set("Value").to("bar").build()
    )

    val sc = ScioContext(options)
    val data = sc.parallelize(mutations)

    SpannerWrite(config).writeWithContext(data, SpannerWrite.WriteParam())
    sc.close()

    val txn = dbClient.readOnlyTransaction()
    val read = txn.read(table, KeySet.all(), Seq("Key", "Value").asJava)

    val results: List[Struct] = List(
      { read.next(); read.getCurrentRowAsStruct },
      { read.next(); read.getCurrentRowAsStruct }
    )

    val expectedOut = List(
      Struct.newBuilder().set("Key").to(1L).set("Value").to("foo").build(),
      Struct.newBuilder().set("Key").to(2).set("Value").to("bar").build()
    )

    results should contain theSameElementsAs expectedOut
  }

  it should "perform reads from table" in {
    val table = s"${tablePrefix}_2"
    val mutations = Seq(
      Mutation.newInsertBuilder(table).set("Key").to(3L).set("Value").to("foo").build(),
      Mutation.newInsertBuilder(table).set("Key").to(4L).set("Value").to("bar").build()
    )

    dbClient.write(mutations.asJava)

    val sc = ScioContext(options)
    val read = SpannerRead(config).readWithContext(
      sc,
      SpannerRead.ReadParam(
        readMethod = SpannerRead.FromTable(table, Seq("Key", "Value")),
        withTransaction = true
      )
    )

    val result = read.materialize.map(_.value.toList)
    sc.close()

    val expectedOut = List(
      Struct.newBuilder()
        .set("Key").to(3L)
        .set("Value").to("foo")
        .build(),
      Struct.newBuilder()
        .set("Key").to(4L)
        .set("Value").to("bar")
        .build())

    val awaited = Await.result(result, 10.seconds)

    awaited should contain theSameElementsAs expectedOut
  }

  it should "perform reads from query" in {
    val table = s"${tablePrefix}_3"
    val mutations = Seq(
      Mutation.newInsertBuilder(table).set("Key").to(5L).set("Value").to("foo").build(),
      Mutation.newInsertBuilder(table).set("Key").to(6L).set("Value").to("bar").build()
    )

    dbClient.write(mutations.asJava)

    val sc = ScioContext(options)
    val read = SpannerRead(config).readWithContext(
      sc,
      SpannerRead.ReadParam(
        readMethod = SpannerRead.FromQuery(s"SELECT Key, Value FROM $table"),
        withBatching = true
      )
    )

    val result = read.materialize.map(_.value.toList)

    sc.close()

    val expectedOut = List(
      Struct.newBuilder()
        .set("Key").to(5L)
        .set("Value").to("foo")
        .build(),
      Struct.newBuilder()
        .set("Key").to(6L)
        .set("Value").to("bar")
        .build())

    Await.result(result, 10.seconds) should contain theSameElementsAs expectedOut
  }
}
