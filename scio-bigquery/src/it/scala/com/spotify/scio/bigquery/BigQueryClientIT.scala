/*
 * Copyright (c) 2016 Spotify AB.
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

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.dataflow.sdk.io.BigQueryIO
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
    bq.extractLocation(query.format("us")) should equal ("US")
    bq.extractLocation(query.format("eu")) should equal ("EU")
  }

  it should "work with SQL syntax" in {
    val query = "SELECT word FROM `data-integration-test.samples_%s.shakespeare`"
    bq.extractLocation(query.format("us")) should equal ("US")
    bq.extractLocation(query.format("eu")) should equal ("EU")
  }

  "extractTables" should "work with legacy syntax" in {
    val tableSpec = BigQueryIO.parseTableSpec("bigquery-public-data:samples.shakespeare")
    bq.extractTables(legacyQuery) should equal (Set(tableSpec))
  }

  it should "work with SQL syntax" in {
    val tableSpec = BigQueryIO.parseTableSpec("bigquery-public-data:samples.shakespeare")
    bq.extractTables(sqlQuery) should equal (Set(tableSpec))
  }

  "getQuerySchema" should "work with legacy syntax" in {
    val expected = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING").setMode("REQUIRED"),
      new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("REQUIRED")
    ).asJava)
    bq.getQuerySchema(legacyQuery) should equal (expected)
  }

  it should "work with SQL syntax" in {
    val expected = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING").setMode("NULLABLE"),
      new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("NULLABLE")
    ).asJava)
    bq.getQuerySchema(sqlQuery) should equal (expected)
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail invalid legacy syntax" in {
    (the [GoogleJsonResponseException] thrownBy {
      bq.getQuerySchema("SELECT word, count FROM [bigquery-public-data:samples.shakespeare]")
    }).getDetails.getCode should be (400)
  }

  it should "fail invalid SQL syntax" in {
    (the [GoogleJsonResponseException] thrownBy {
      bq.getQuerySchema("SELECT word, count FROM `bigquery-public-data.samples.shakespeare`")
    }).getDetails.getCode should be (400)
  }
  // scalastyle:on no.whitespace.before.left.bracket

  "getQueryRows" should "work with legacy syntax" in {
    val rows = bq.getQueryRows(legacyQuery).toList
    rows.size should be (10)
    all(rows.map(_.keySet().asScala)) should equal (Set("word", "word_count"))
  }

  it should "work with SQL syntax" in {
    val rows = bq.getQueryRows(sqlQuery).toList
    rows.size should be (10)
    all(rows.map(_.keySet().asScala)) should equal (Set("word", "word_count"))
  }

  "getTableSchema" should "work" in {
    val schema = bq.getTableSchema("bigquery-public-data:samples.shakespeare")
    val fields = schema.getFields.asScala
    fields.size should be (4)
    fields.map(_.getName) should equal (Seq("word", "word_count", "corpus", "corpus_date"))
    fields.map(_.getType) should equal (Seq("STRING", "INTEGER", "STRING", "INTEGER"))
    fields.map(_.getMode) should equal (Seq("REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED"))
  }

  "getTableRows" should "work" in {
    val rows = bq.getTableRows("bigquery-public-data:samples.shakespeare").take(10).toList
    val columns = Set("word", "word_count", "corpus", "corpus_date")
    all(rows.map(_.keySet().asScala)) should equal (columns)
  }

}
