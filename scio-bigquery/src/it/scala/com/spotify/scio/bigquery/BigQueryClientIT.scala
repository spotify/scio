/*
 * Copyright 2019 Spotify AB.
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
import com.spotify.scio.bigquery.client.BigQuery
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._
import scala.util.Success

// scio-test/it:runMain com.spotify.scio.PopulateTestData to re-populate data for integration tests
class BigQueryClientIT extends AnyFlatSpec with Matchers {
  private[this] val bq = BigQuery.defaultInstance()

  val legacyQuery =
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] LIMIT 10"
  val sqlQuery =
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"

  "QueryService.run" should "run DML queries" in {
    val schema =
      BigQueryUtil.parseSchema("""
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
    val table = bq.tables.createTemporary(location = "EU")
    val tableRef = bq.load.json(sources, table.asTableSpec, schema = Some(schema))
    tableRef.map { ref =>
      val insertQuery = s"insert into `${ref.asTableSpec}` values (1603, 'alien', 9000, 'alien')"
      bq.query.run(
        insertQuery,
        createDisposition = null,
        writeDisposition = null
      )

      val deleteQuery = s"delete from `${ref.asTableSpec}` where word = 'alien'"
      bq.query.run(
        deleteQuery,
        createDisposition = null,
        writeDisposition = null
      )
    }
  }

  "QueryService.extractLocation" should "work with legacy syntax" in {
    val query = "SELECT word FROM [data-integration-test:samples_%s.shakespeare]"
    bq.query.extractLocation(query.format("eu")) shouldBe Some("EU")
    bq.query.extractLocation(query.format("us")) shouldBe Some("US")
  }

  it should "work with SQL syntax" in {
    val query = "SELECT word FROM `data-integration-test.samples_%s.shakespeare`"
    bq.query.extractLocation(query.format("eu")) shouldBe Some("EU")
    bq.query.extractLocation(query.format("us")) shouldBe Some("US")
  }

  it should "support missing source tables" in {
    bq.query.extractLocation("SELECT 6") shouldBe Some("US")
  }

  "QueryService.extractTables" should "work with legacy syntax" in {
    val tableSpec = BigQueryHelpers.parseTableSpec("bigquery-public-data:samples.shakespeare")
    bq.query.extractTables(legacyQuery) shouldBe Set(tableSpec)
  }

  it should "work with SQL syntax" in {
    val tableSpec = BigQueryHelpers.parseTableSpec("bigquery-public-data:samples.shakespeare")
    bq.query.extractTables(sqlQuery) shouldBe Set(tableSpec)
  }

  "QueryService.getSchema" should "work with legacy syntax" in {
    val expected = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("word").setType("STRING").setMode("REQUIRED"),
        new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("REQUIRED")
      ).asJava
    )
    bq.query.schema(legacyQuery) shouldBe expected
  }

  it should "work with SQL syntax" in {
    val expected = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("word").setType("STRING").setMode("NULLABLE"),
        new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("NULLABLE")
      ).asJava
    )
    bq.query.schema(sqlQuery) shouldBe expected
  }

  it should "fail invalid legacy syntax" in {
    (the[GoogleJsonResponseException] thrownBy {
      bq.query.schema("SELECT word, count FROM [bigquery-public-data:samples.shakespeare]")
    }).getDetails.getCode shouldBe 400
  }

  it should "fail invalid SQL syntax" in {
    (the[GoogleJsonResponseException] thrownBy {
      bq.query.schema("SELECT word, count FROM `bigquery-public-data.samples.shakespeare`")
    }).getDetails.getCode shouldBe 400
  }

  "QueryService.getRows" should "work with legacy syntax" in {
    val rows = bq.query.rows(legacyQuery).toList
    rows.size shouldBe 10
    all(rows.map(_.keySet().asScala)) shouldBe Set("word", "word_count")
  }

  it should "work with SQL syntax" in {
    val rows = bq.query.rows(sqlQuery).toList
    rows.size shouldBe 10
    all(rows.map(_.keySet().asScala)) shouldBe Set("word", "word_count")
  }

  "TableService.getTableSchema" should "work" in {
    val schema = bq.tables.schema("bigquery-public-data:samples.shakespeare")
    val fields = schema.getFields.asScala
    fields.size shouldBe 4
    fields.map(_.getName) shouldBe Seq("word", "word_count", "corpus", "corpus_date")
    fields.map(_.getType) shouldBe Seq("STRING", "INTEGER", "STRING", "INTEGER")
    fields.map(_.getMode) shouldBe Seq("REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED")
  }

  "TableService.getRows" should "work" in {
    val rows =
      bq.tables.rows(Table.Spec("bigquery-public-data:samples.shakespeare")).take(10).toList
    val columns = Set("word", "word_count", "corpus", "corpus_date")
    all(rows.map(_.keySet().asScala)) shouldBe columns
  }

  "Load.csv" should "work" in {
    val schema =
      BigQueryUtil.parseSchema("""
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
    val table = bq.tables.createTemporary(location = "EU")
    val tableRef =
      bq.load.csv(sources, table.asTableSpec, skipLeadingRows = 1, schema = Some(schema))
    val createdTable = tableRef.map(bq.tables.table)
    createdTable.map(_.getNumRows.intValue()) shouldBe Success(10)
    tableRef.map(bq.tables.delete)
  }

  "Load.json" should "work" in {
    val schema =
      BigQueryUtil.parseSchema("""
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
    val table = bq.tables.createTemporary(location = "EU")
    val tableRef = bq.load.json(sources, table.asTableSpec, schema = Some(schema))
    val createdTable = tableRef.map(bq.tables.table)
    createdTable.map(_.getNumRows.intValue()) shouldBe Success(10)
    tableRef.map(bq.tables.delete)
  }

  "Load.avro" should "work" in {
    val sources = List("gs://data-integration-test-eu/shakespeare-sample-10.avro")
    val table = bq.tables.createTemporary(location = "EU")
    val tableRef = bq.load.avro(sources, table.asTableSpec)
    val createdTable = tableRef.map(bq.tables.table)
    createdTable.map(_.getNumRows.intValue()) shouldBe Success(10)
    tableRef.map(bq.tables.delete)
  }

  "extract.asCsv" should "work" in {
    val sourceTable = "bigquery-public-data:samples.shakespeare"
    val (bucket, prefix) = ("data-integration-test-eu", s"extract/csv/${UUID.randomUUID}")
    GcsUtils.exists(bucket, prefix) shouldBe false
    val destination = List(
      s"gs://$bucket/$prefix"
    )
    bq.extract.asCsv(sourceTable, destination)
    GcsUtils.exists(bucket, prefix) shouldBe true
    GcsUtils.remove(bucket, prefix)
  }

  "extract.asJson" should "work" in {
    val sourceTable = "bigquery-public-data:samples.shakespeare"
    val (bucket, prefix) = ("data-integration-test-eu", s"extract/json/${UUID.randomUUID}")
    GcsUtils.exists(bucket, prefix) shouldBe false
    val destination = List(
      s"gs://$bucket/$prefix"
    )
    bq.extract.asJson(sourceTable, destination)
    GcsUtils.exists(bucket, prefix) shouldBe true
    GcsUtils.remove(bucket, prefix)
  }

  "extract.asAvro" should "work" in {
    val sourceTable = "bigquery-public-data:samples.shakespeare"
    val (bucket, prefix) = ("data-integration-test-eu", s"extract/avro/${UUID.randomUUID}")
    GcsUtils.exists(bucket, prefix) shouldBe false
    val destination = List(
      s"gs://$bucket/$prefix"
    )
    bq.extract.asAvro(sourceTable, destination)
    GcsUtils.exists(bucket, prefix) shouldBe true
    GcsUtils.remove(bucket, prefix)
  }

  object GcsUtils {
    private val storage = StorageOptions.getDefaultInstance.getService

    private def list(bucket: String, prefix: String): Iterable[Blob] =
      storage
        .list(bucket, BlobListOption.prefix(prefix))
        .iterateAll()
        .asScala

    def exists(bucket: String, prefix: String): Boolean =
      list(bucket, prefix).nonEmpty

    def remove(bucket: String, prefix: String): Unit = {
      storage.delete(list(bucket, prefix).map(_.getBlobId).toSeq: _*)
      ()
    }
  }
}
