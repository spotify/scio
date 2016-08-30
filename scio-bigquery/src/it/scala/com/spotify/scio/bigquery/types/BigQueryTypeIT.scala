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

package com.spotify.scio.bigquery.types

import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.spotify.scio.bigquery.BigQueryClient
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

object BigQueryTypeIT {
  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] WHERE word = 'Romeo'")
  class LegacyT

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word = 'Romeo'")
  class SqlT

  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class FromTableT

  @BigQueryType.toTable
  case class ToTableT(word: String, word_count: Int)
}

class BigQueryTypeIT extends FlatSpec with Matchers {

  import BigQueryTypeIT._

  val bq = BigQueryClient.defaultInstance()

  val legacyQuery =
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] WHERE word = 'Romeo'"
  val sqlQuery =
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word = 'Romeo'"

  "fromQuery" should "work with legacy syntax" in {
    val bqt= BigQueryType[LegacyT]
    bqt.isQuery should be (true)
    bqt.isTable should be (false)
    bqt.query should be (Some(legacyQuery))
    bqt.table should be (None)
    val fields = bqt.schema.getFields.asScala
    fields.size should be (2)
    fields.map(_.getName) should equal (Seq("word", "word_count"))
    fields.map(_.getType) should equal (Seq("STRING", "INTEGER"))
    fields.map(_.getMode) should equal (Seq("REQUIRED", "REQUIRED"))
  }

  it should "work with SQL syntax" in {
    val bqt= BigQueryType[SqlT]
    bqt.isQuery should be (true)
    bqt.isTable should be (false)
    bqt.query should be (Some(sqlQuery))
    bqt.table should be (None)
    val fields = bqt.schema.getFields.asScala
    fields.size should be (2)
    fields.map(_.getName) should equal (Seq("word", "word_count"))
    fields.map(_.getType) should equal (Seq("STRING", "INTEGER"))
    fields.map(_.getMode) should equal (Seq("NULLABLE", "NULLABLE"))
  }

  it should "round trip rows with legacy syntax" in {
    val bqt= BigQueryType[LegacyT]
    val rows = bq.getQueryRows(legacyQuery).toList
    val typed = Seq(LegacyT("Romeo", 117L))
    rows.map(bqt.fromTableRow) should equal (typed)
    typed.map(bqt.toTableRow).map(bqt.fromTableRow) should equal (typed)
  }

  it should "round trip rows with SQL syntax" in {
    val bqt= BigQueryType[SqlT]
    val rows = bq.getQueryRows(sqlQuery).toList
    val typed = Seq(SqlT(Some("Romeo"), Some(117L)))
    rows.map(bqt.fromTableRow) should equal (typed)
    typed.map(bqt.toTableRow).map(bqt.fromTableRow) should equal (typed)
  }

  "fromTable" should "work" in {
    val bqt = BigQueryType[FromTableT]
    val tableRef = BigQueryIO.parseTableSpec("bigquery-public-data:samples.shakespeare")
    bqt.isQuery should be (false)
    bqt.isTable should be (true)
    bqt.query should be (None)
    bqt.table should be (Some(tableRef))
    val fields = bqt.schema.getFields.asScala
    fields.size should be (4)
    fields.map(_.getName) should equal (Seq("word", "word_count", "corpus", "corpus_date"))
    fields.map(_.getType) should equal (Seq("STRING", "INTEGER", "STRING", "INTEGER"))
    fields.map(_.getMode) should equal (Seq("REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED"))
  }

  "toTable" should "work" in {
    val bqt = BigQueryType[ToTableT]
    bqt.isQuery should be (false)
    bqt.isTable should be (false)
    bqt.query should be (None)
    bqt.table should be (None)
    val fields = bqt.schema.getFields.asScala
    fields.size should be (2)
    fields.map(_.getName) should equal (Seq("word", "word_count"))
    fields.map(_.getType) should equal (Seq("STRING", "INTEGER"))
    fields.map(_.getMode) should equal (Seq("REQUIRED", "REQUIRED"))
  }

}
