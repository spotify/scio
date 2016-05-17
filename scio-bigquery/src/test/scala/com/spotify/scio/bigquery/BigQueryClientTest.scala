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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class BigQueryClientTest extends FlatSpec with Matchers {

  val bq = new BigQueryClient(
    "scio-bigquery",
    this.getClass.getResourceAsStream("/scio-bigquery-f519b8001e82.json"))

  "getQuerySchema" should "work" in {
    val sqlQuery = "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare`"
    val fields = bq.getQuerySchema(sqlQuery).getFields.asScala
    fields.size shouldBe 2
    fields.map(_.getName) should equal (Seq("word", "word_count"))
    fields.map(_.getType) should equal (Seq("STRING", "INTEGER"))
    fields.map(_.getMode) should equal (Seq("NULLABLE", "NULLABLE"))
  }

  // TODO: figure out how to test getQueryRows without billing information

  "getQueryTables" should "work" in {
    val sqlQuery = """
                     |SELECT s.title, c.text
                     |FROM `bigquery-public-data.hacker_news.stories` AS s
                     |JOIN `bigquery-public-data.hacker_news.comments` AS c
                     |ON s.`by` = c.`by`
                   """.stripMargin
    bq.getQueryTables(sqlQuery).map(BigQueryIO.toTableSpec).toSet should equal (Set(
      "bigquery-public-data:hacker_news.stories",
      "bigquery-public-data:hacker_news.comments"
    ))
  }

  "getTableSchema" should "work" in {
    val tableRef = BigQueryIO.parseTableSpec("bigquery-public-data:samples.shakespeare")
    val fields = bq.getTableSchema(tableRef).getFields.asScala
    fields.size shouldBe 4
    fields.map(_.getName) should equal (Seq("word", "word_count", "corpus", "corpus_date"))
    fields.map(_.getType) should equal (Seq("STRING", "INTEGER", "STRING", "INTEGER"))
    fields.map(_.getMode) should equal (Seq("REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED"))
  }

  "getTableRows" should "work" in {
    val tableRef = BigQueryIO.parseTableSpec("bigquery-public-data:samples.shakespeare")
    val row = bq.getTableRows(tableRef).next()
    row.keySet().asScala should equal (Set("word", "word_count", "corpus", "corpus_date"))
  }

}
