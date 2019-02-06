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

import scala.util.Random

object SpannerIOIT {
  private val projectId = "data-integration-test"
  private val options = PipelineOptionsFactory.create()
  private val config: SpannerConfig = SpannerConfig
    .create()
    .withProjectId(projectId)
    .withDatabaseId(s"io_it_${Random.nextInt}")
    .withInstanceId("spanner-it")

  private val adminClient = Spanner.adminClient(projectId)
  private val dbClient = Spanner.databaseClient(config)

  private final case class FakeSpannerData(asMutations: Seq[Mutation], asStructs: Seq[Struct])
  private def fakeData(tableName: String) = FakeSpannerData(
    Seq(
      Mutation
        .newInsertBuilder(tableName)
        .set("Key")
        .to(1L)
        .set("Value")
        .to("foo")
        .build(),
      Mutation
        .newInsertBuilder(tableName)
        .set("Key")
        .to(2L)
        .set("Value")
        .to("bar")
        .build()
    ),
    Seq(Struct
          .newBuilder()
          .set("Key")
          .to(1L)
          .set("Value")
          .to("foo")
          .build(),
        Struct
          .newBuilder()
          .set("Key")
          .to(2L)
          .set("Value")
          .to("bar")
          .build())
  )
}

class SpannerIOIT extends FlatSpec with Matchers with BeforeAndAfterAll {
  import SpannerIOIT._

  override def beforeAll(): Unit = {
    adminClient
      .createDatabase(
        config.getInstanceId.get(),
        config.getDatabaseId.get(),
        List(
          "CREATE TABLE read_query_test ( Key INT64, Value STRING(MAX) ) PRIMARY KEY (Key)",
          "CREATE TABLE read_table_test ( Key INT64, Value STRING(MAX) ) PRIMARY KEY (Key)",
          "CREATE TABLE write_test ( Key INT64, Value STRING(MAX) ) PRIMARY KEY (Key)"
        ).asJava
      )
      .waitFor()
  }

  override def afterAll(): Unit =
    adminClient.dropDatabase(config.getInstanceId.get(), config.getDatabaseId.get())

  private class PopulatedSpannerTable(val tableName: String) {
    val spannerRows: FakeSpannerData = fakeData(tableName)
    dbClient.write(spannerRows.asMutations.asJava)
  }

  private class ReadableSpannerTable(val tableName: String) {
    val writeData: FakeSpannerData = fakeData(tableName)

    lazy val readOperationResults: Seq[Struct] = {
      val txn = dbClient
        .readOnlyTransaction()
        .read(tableName, KeySet.all(), Seq("Key", "Value").asJava)

      for (_ <- writeData.asStructs) yield { txn.next(); txn.getCurrentRowAsStruct }
    }
  }

  "SpannerIO" should "perform writes" in new ReadableSpannerTable("write_test") {
    private val sc = ScioContext(options)

    SpannerWrite(config)
      .writeWithContext(sc.parallelize(writeData.asMutations), SpannerWrite.WriteParam())

    sc.close().waitUntilDone()
    readOperationResults should contain theSameElementsAs writeData.asStructs
  }

  it should "perform reads from table" in new PopulatedSpannerTable("read_table_test") {
    private val sc = ScioContext(options)

    private val read = SpannerRead(config)
      .readWithContext(
        sc,
        SpannerRead.ReadParam(
          readMethod = SpannerRead.FromTable(tableName, Seq("Key", "Value")),
          withTransaction = true,
          withBatching = true
        )
      )
      .materialize

    val scioResult = sc.close().waitUntilDone()
    scioResult.tap(read).value.toList should contain theSameElementsAs spannerRows.asStructs
  }

  it should "perform reads from query" in new PopulatedSpannerTable("read_query_test") {
    private val sc = ScioContext(options)

    private val read = SpannerRead(config)
      .readWithContext(
        sc,
        SpannerRead.ReadParam(
          readMethod = SpannerRead.FromQuery(s"SELECT Key, Value FROM $tableName"),
          withTransaction = true,
          withBatching = true
        )
      )
      .materialize

    val scioResult = sc.close().waitUntilDone()
    scioResult.tap(read).value.toList should contain theSameElementsAs spannerRows.asStructs
  }
}
