/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.bigquery

import java.util.UUID

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Blob, StorageOptions}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class BigQueryClientIT extends FlatSpec with Matchers {

  val bq = BigQueryClient.defaultInstance()

  val legacyQuery =
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] LIMIT 10"
  val sqlQuery =
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"

  "extractLocation" should "work with legacy syntax" in {
    val query = "SELECT word FROM [data-integration-test:samples_%s.shakespeare]"
    bq.extractLocation(query.format("us")) shouldBe Some("US")
    bq.extractLocation(query.format("eu")) shouldBe Some("EU")
  }

  it should "work with SQL syntax" in {
    val query = "SELECT word FROM `data-integration-test.samples_%s.shakespeare`"
    bq.extractLocation(query.format("us")) shouldBe Some("US")
    bq.extractLocation(query.format("eu")) shouldBe Some("EU")
  }

  it should "support missing source tables" in {
    bq.extractLocation("SELECT 6") shouldBe None
  }

  "extractTables" should "work with legacy syntax" in {
    val tableSpec = BigQueryHelpers.parseTableSpec("bigquery-public-data:samples.shakespeare")
    bq.extractTables(legacyQuery) shouldBe Set(tableSpec)
  }

  it should "work with SQL syntax" in {
    val tableSpec = BigQueryHelpers.parseTableSpec("bigquery-public-data:samples.shakespeare")
    bq.extractTables(sqlQuery) shouldBe Set(tableSpec)
  }

  "getQuerySchema" should "work with legacy syntax" in {
    val expected = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING").setMode("REQUIRED"),
      new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("REQUIRED")
    ).asJava)
    bq.getQuerySchema(legacyQuery) shouldBe expected
  }

  it should "work with SQL syntax" in {
    val expected = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING").setMode("NULLABLE"),
      new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("NULLABLE")
    ).asJava)
    bq.getQuerySchema(sqlQuery) shouldBe expected
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail invalid legacy syntax" in {
    (the [GoogleJsonResponseException] thrownBy {
      bq.getQuerySchema("SELECT word, count FROM [bigquery-public-data:samples.shakespeare]")
    }).getDetails.getCode shouldBe 400
  }

  it should "fail invalid SQL syntax" in {
    (the [GoogleJsonResponseException] thrownBy {
      bq.getQuerySchema("SELECT word, count FROM `bigquery-public-data.samples.shakespeare`")
    }).getDetails.getCode shouldBe 400
  }
  // scalastyle:on no.whitespace.before.left.bracket

  "getQueryRows" should "work with legacy syntax" in {
    val rows = bq.getQueryRows(legacyQuery).toList
    rows.size shouldBe 10
    all(rows.map(_.keySet().asScala)) shouldBe Set("word", "word_count")
  }

  it should "work with SQL syntax" in {
    val rows = bq.getQueryRows(sqlQuery).toList
    rows.size shouldBe 10
    all(rows.map(_.keySet().asScala)) shouldBe Set("word", "word_count")
  }

  "getTableSchema" should "work" in {
    val schema = bq.getTableSchema("bigquery-public-data:samples.shakespeare")
    val fields = schema.getFields.asScala
    fields.size shouldBe 4
    fields.map(_.getName) shouldBe Seq("word", "word_count", "corpus", "corpus_date")
    fields.map(_.getType) shouldBe Seq("STRING", "INTEGER", "STRING", "INTEGER")
    fields.map(_.getMode) shouldBe Seq("REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED")
  }

  "getTableRows" should "work" in {
    val rows = bq.getTableRows("bigquery-public-data:samples.shakespeare").take(10).toList
    val columns = Set("word", "word_count", "corpus", "corpus_date")
    all(rows.map(_.keySet().asScala)) shouldBe columns
  }

  "loadTableFromCsv" should "work" in {
    val schema = BigQueryUtil.parseSchema(
      """
        |{
        |  "fields": [
        |    {"mode": "NULLABLE", "name": "word", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "word_count", "type": "INTEGER"},
        |    {"mode": "NULLABLE", "name": "corpus", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "corpus_date", "type": "INTEGER"}
        |  ]
        |}
      """.stripMargin)
    val sources = List("gs://data-integration-test-eu/shakespeare-sample-10.csv")
    val table = bq.temporaryTable(location = "US")
    val tableRef = bq.loadTableFromCsv(sources, table.asTableSpec, skipLeadingRows = 1,
      schema = Some(schema))
    val createdTable = bq.getTable(tableRef)
    createdTable.getNumRows.intValue() shouldBe 10
    bq.deleteTable(tableRef)
  }

  "loadTableFromJson" should "work" in {
    val schema = BigQueryUtil.parseSchema(
      """
        |{
        |  "fields": [
        |    {"mode": "NULLABLE", "name": "word", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "word_count", "type": "INTEGER"},
        |    {"mode": "NULLABLE", "name": "corpus", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "corpus_date", "type": "INTEGER"}
        |  ]
        |}
      """.stripMargin)
    val sources = List("gs://data-integration-test-eu/shakespeare-sample-10.json")
    val table = bq.temporaryTable(location = "US")
    val tableRef = bq.loadTableFromJson(sources, table.asTableSpec, schema = Some(schema))
    val createdTable = bq.getTable(tableRef)
    createdTable.getNumRows.intValue() shouldBe 10
    bq.deleteTable(tableRef)
  }

  "loadTableFromAvro" should "work" in {
    val sources = List("gs://data-integration-test-eu/shakespeare-sample-10.avro")
    val table = bq.temporaryTable(location = "US")
    val tableRef = bq.loadTableFromAvro(sources, table.asTableSpec)
    val createdTable = bq.getTable(tableRef)
    createdTable.getNumRows.intValue() shouldBe 10
    bq.deleteTable(tableRef)
  }

  "exportTableAsCsv" should "work" in {
    val sourceTable = "bigquery-public-data:samples.shakespeare"
    val (bucket, prefix) = ("data-integration-test-eu", s"extract/csv/${UUID.randomUUID}")
    GcsUtils.exists(bucket, prefix) shouldBe false
    val destination = List(
      s"gs://$bucket/$prefix"
    )
    bq.exportTableAsCsv(sourceTable, destination)
    GcsUtils.exists(bucket, prefix) shouldBe true
    GcsUtils.remove(bucket, prefix)
  }

  "exportTableAsJson" should "work" in {
    val sourceTable = "bigquery-public-data:samples.shakespeare"
    val (bucket, prefix) = ("data-integration-test-eu", s"extract/json/${UUID.randomUUID}")
    GcsUtils.exists(bucket, prefix) shouldBe false
    val destination = List(
      s"gs://$bucket/$prefix"
    )
    bq.exportTableAsJson(sourceTable, destination)
    GcsUtils.exists(bucket, prefix) shouldBe true
    GcsUtils.remove(bucket, prefix)
  }

  "exportTableAsAvro" should "work" in {
    val sourceTable = "bigquery-public-data:samples.shakespeare"
    val (bucket, prefix) = ("data-integration-test-eu", s"extract/avro/${UUID.randomUUID}")
    GcsUtils.exists(bucket, prefix) shouldBe false
    val destination = List(
      s"gs://$bucket/$prefix"
    )
    bq.exportTableAsAvro(sourceTable, destination)
    GcsUtils.exists(bucket, prefix) shouldBe true
    GcsUtils.remove(bucket, prefix)
  }

  object GcsUtils {

    private val storage = StorageOptions.getDefaultInstance.getService

    private def list(bucket: String, prefix: String): Iterable[Blob] = {
      storage.list(bucket, BlobListOption.prefix(prefix))
        .iterateAll()
        .asScala
    }

    def exists(bucket: String, prefix: String): Boolean = {
      list(bucket, prefix)
        .nonEmpty
    }

    def remove(bucket: String, prefix: String): Unit = {
      storage.delete(list(bucket, prefix).map(_.getBlobId).toSeq: _*)
    }
  }

}
