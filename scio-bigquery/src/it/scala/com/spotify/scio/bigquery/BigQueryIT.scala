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

import com.spotify.scio.bigquery.types._
import org.scalatest.Inspectors.forAll
import org.scalatest._

object BigQueryIT {

  val tableRef = "bigquery-public-data:samples.shakespeare"
  val legacyQuery =
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] LIMIT 10"
  val sqlQuery =
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"

  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class Shakespeare

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10")
  class WordCount

}

class BigQueryIT extends FlatSpec with Matchers {

  import BigQueryIT._

  // =======================================================================
  // Integration test with mock data
  // =======================================================================

  "MockBigQuery" should "support mock data" in {
    def shakespeare(w: String, wc: Long, c: String, cd: Long): TableRow =
      TableRow("word" -> w, "word_count" -> wc, "corpus" -> c, "corpus_date" -> cd)

    // BigQuery result TableRow treats integers as strings
    def wordCount(w: String, wc: Long): TableRow =
      TableRow("word" -> w, "word_count" -> wc.toString)

    val inData = Seq(
      shakespeare("i", 10, "kinglear", 1600),
      shakespeare("thou", 20, "kinglear", 1600),
      shakespeare("thy", 30, "kinglear", 1600))
    val expected = Seq(wordCount("i", 10), wordCount("thou", 20), wordCount("thy", 30))

    val mbq = MockBigQuery()
    mbq.mockTable(tableRef).withData(inData)
    mbq.queryResult(legacyQuery) should contain theSameElementsAs expected
    mbq.queryResult(sqlQuery) should contain theSameElementsAs expected
  }

  // =======================================================================
  // Integration test with type-safe mock data
  // =======================================================================

  it should "support typed BigQuery" in {
    val inData = Seq(
      Shakespeare("i", 10, "kinglear", 1600),
      Shakespeare("thou", 20, "kinglear", 1600),
      Shakespeare("thy", 30, "kinglear", 1600))
    val expected = Seq(
      WordCount(Some("i"), Some(10)),
      WordCount(Some("thou"), Some(20)),
      WordCount(Some("thy"), Some(30)))

    val mbq = MockBigQuery()
    mbq.mockTable(tableRef).withTypedData(inData)
    mbq.typedQueryResult[WordCount](legacyQuery) should contain theSameElementsAs expected
    mbq.typedQueryResult[WordCount](sqlQuery) should contain theSameElementsAs expected
  }

  // =======================================================================
  // Integration test with sample data
  // =======================================================================

  it should "support sample data" in {
    val mbq = MockBigQuery()
    mbq.mockTable(tableRef).withSample(100)
    forAll(mbq.queryResult(sqlQuery)) { r =>
      val word = r.get("word").toString
      word should not be null
      word should not be empty
      r.get("word_count").toString.toInt should be > 0
    }
  }

  // =======================================================================
  // Failure modes
  // =======================================================================

  it should "fail insufficient sample data" in {
    val t = "clouddataflow-readonly:samples.weather_stations"

    the[IllegalArgumentException] thrownBy {
      val mbq = MockBigQuery()
      mbq.mockTable(t).withSample(2000)
    } should have message s"requirement failed: Sample size 1000 != requested 2000"

    the[IllegalArgumentException] thrownBy {
      val mbq = MockBigQuery()
      mbq.mockTable(t).withSample(2000, 5000)
    } should have message s"requirement failed: Sample size 1000 < requested minimal 2000"
  }

  it should "fail duplicate mockTable" in {
    val mbq = MockBigQuery()
    mbq.mockTable(tableRef)
    the[IllegalArgumentException] thrownBy {
      mbq.mockTable(tableRef)
    } should have message s"requirement failed: Table $tableRef already registered for mocking"
  }

  it should "fail duplicate mock data" in {
    val mbq = MockBigQuery()
    val mt = mbq.mockTable(tableRef)
    mt.withData(Nil)
    the[IllegalArgumentException] thrownBy {
      mt.withData(Nil)
    } should have message s"requirement failed: Table $tableRef already populated with mock data"
  }

  it should "fail missing mock data" in {
    val mbq = MockBigQuery()
    mbq.mockTable(tableRef)
    the[RuntimeException] thrownBy {
      mbq.queryResult(sqlQuery)
    } should have message
      "404 Not Found, this is most likely caused by missing source table or mock data"
  }

}
