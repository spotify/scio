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

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

object BigQueryIT {
  val tableRef = "bigquery-public-data:samples.shakespeare"
  val legacyQuery =
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] LIMIT 10"
  val sqlQuery =
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"

  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class Shakespeare

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"
  )
  class WordCount
}

class BigQueryIT extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  import BigQueryIT._

  // =======================================================================
  // Integration test with mock data
  // =======================================================================

  val StagingDatasetPrefix = "scio_bigquery_staging_custom"

  override def beforeEach(): Unit =
    sys.props -= BigQuerySysProps.StagingDatasetPrefix.flag

  override def afterAll(): Unit =
    sys.props -= BigQuerySysProps.StagingDatasetPrefix.flag

  object MockBQData {
    def shakespeare(w: String, wc: Long, c: String, cd: Long): TableRow =
      TableRow("word" -> w, "word_count" -> wc, "corpus" -> c, "corpus_date" -> cd)

    // BigQuery result TableRow treats integers as strings
    def wordCount(w: String, wc: Long): TableRow =
      TableRow("word" -> w, "word_count" -> wc.toString)

    val inData = Seq(
      shakespeare("i", 10, "kinglear", 1600),
      shakespeare("thou", 20, "kinglear", 1600),
      shakespeare("thy", 30, "kinglear", 1600)
    )
    val expected = Seq(wordCount("i", 10), wordCount("thou", 20), wordCount("thy", 30))
  }

  it should "support mock data" in {
    val mbq = MockBigQuery()
    mbq.mockTable(tableRef).withData(MockBQData.inData)
    mbq.queryResult(legacyQuery) should contain theSameElementsAs MockBQData.expected
    mbq.queryResult(sqlQuery) should contain theSameElementsAs MockBQData.expected
  }

  it should "support mock data with custom staging dataset" in {
    val defaultMockedBQ = MockBigQuery()
    defaultMockedBQ.mockTable(tableRef).withData(Seq())

    sys.props += BigQuerySysProps.StagingDatasetPrefix.flag -> StagingDatasetPrefix
    val mockedBQ = MockBigQuery()
    mockedBQ.mockTable(tableRef).withData(MockBQData.inData)
    mockedBQ.queryResult(legacyQuery) should contain theSameElementsAs MockBQData.expected
    mockedBQ.queryResult(sqlQuery) should contain theSameElementsAs MockBQData.expected

    // making sure nothing is written using default staging tables
    defaultMockedBQ.queryResult(legacyQuery) should contain theSameElementsAs Seq()
    defaultMockedBQ.queryResult(sqlQuery) should contain theSameElementsAs Seq()
  }

  it should "support DML on mock data" in {
    val writableTableRef = "data-integration-test.samples_eu.shakespeare"
    val allRowsQuery = s"SELECT word, word_count FROM `$writableTableRef` LIMIT 10"
    val mbq = MockBigQuery()
    mbq.mockTable(writableTableRef).withData(MockBQData.inData)
    val insertQuery = s"INSERT INTO `$writableTableRef` (word, word_count) VALUES ('cat', 2)"
    mbq.runDML(insertQuery)
    mbq.queryResult(allRowsQuery) should contain theSameElementsAs MockBQData.expected ++ Seq(
      MockBQData.wordCount("cat", 2)
    )
    val updateQuery = s"UPDATE `$writableTableRef` SET word_count = 4 WHERE word = 'cat'"
    mbq.runDML(updateQuery)
    mbq.queryResult(allRowsQuery) should contain theSameElementsAs MockBQData.expected ++ Seq(
      MockBQData.wordCount("cat", 4)
    )
    val deleteQuery = s"DELETE FROM `$writableTableRef` WHERE word = 'cat'"
    mbq.runDML(deleteQuery)
    mbq.queryResult(allRowsQuery) should contain theSameElementsAs MockBQData.expected

    an[IllegalArgumentException] shouldBe thrownBy {
      mbq.runDML(allRowsQuery)
    }
  }

  object MockBQWildcardData {
    val prefix = "bigquery-public-data.noaa_gsod.gsod20"
    val wildcardSqlQuery: String =
      """SELECT
        |  max,
        |  year,
        |  mo,
        |  da,
        |FROM
        |  `bigquery-public-data.noaa_gsod.gsod20*`
        |WHERE
        |  max != 9999.9 # code for missing data
        |  AND _TABLE_SUFFIX BETWEEN '20' AND '29'
        |ORDER BY
        |  max DESC
        |""".stripMargin

    def gsod(max: Double, year: Int, mo: Int, da: Int): TableRow =
      TableRow(
        "max" -> max,
        "year" -> year.toString,
        "mo" -> mo.toString,
        "da" -> da.toString
      )

    val suffixData = Map(
      "21" -> Seq(
        gsod(54.2, 2021, 6, 30),
        gsod(53.2, 2021, 6, 22),
        gsod(53.1, 2021, 7, 1),
        gsod(9999.9, 2021, 1, 1)
      ),
      "20" -> Seq(
        gsod(54.0, 2020, 6, 8),
        gsod(52.2, 2020, 7, 31),
        gsod(52.1, 2020, 7, 30),
        gsod(9999.9, 2020, 1, 1)
      ),
      // not part of the 20s decade. will be ignored
      "19" -> Seq(
        gsod(54.5, 2019, 8, 29),
        gsod(54.4, 2019, 8, 30),
        gsod(54.4, 2019, 8, 29),
        gsod(9999.9, 2019, 1, 1)
      )
    )

    val expected = Seq(
      gsod(54.2, 2021, 6, 30),
      gsod(54.0, 2020, 6, 8),
      gsod(53.2, 2021, 6, 22),
      gsod(53.1, 2021, 7, 1),
      gsod(52.2, 2020, 7, 31),
      gsod(52.1, 2020, 7, 30)
    )
  }

  // see https://cloud.google.com/bigquery/docs/querying-wildcard-tables
  it should "support wildcard tables" in {
    val mbq = MockBigQuery()
    MockBQWildcardData.suffixData.foreach { case (suffix, inData) =>
      mbq.mockWildcardTable(MockBQWildcardData.prefix, suffix).withData(inData)
    }
    mbq.queryResult(
      MockBQWildcardData.wildcardSqlQuery
    ) should contain theSameElementsInOrderAs MockBQWildcardData.expected
  }

  it should "support wildcard tables with custom staging dataset" in {
    val defaultMockedBQ = MockBigQuery()
    defaultMockedBQ.mockTable(tableRef).withData(Seq())

    sys.props += BigQuerySysProps.StagingDatasetPrefix.flag -> StagingDatasetPrefix
    val mockedBQ = MockBigQuery()
    MockBQWildcardData.suffixData.foreach { case (suffix, inData) =>
      mockedBQ.mockWildcardTable(MockBQWildcardData.prefix, suffix).withData(inData)
    }
    mockedBQ.queryResult(
      MockBQWildcardData.wildcardSqlQuery
    ) should contain theSameElementsInOrderAs MockBQWildcardData.expected

    // making sure nothing is written using default staging tables
    defaultMockedBQ.queryResult(legacyQuery) should contain theSameElementsAs Seq()
    defaultMockedBQ.queryResult(sqlQuery) should contain theSameElementsAs Seq()
  }

//  // =======================================================================
//  // Integration test with type-safe mock data
//  // =======================================================================

  it should "support typed BigQuery" in {
    val inData = Seq(
      Shakespeare("i", 10, "kinglear", 1600),
      Shakespeare("thou", 20, "kinglear", 1600),
      Shakespeare("thy", 30, "kinglear", 1600)
    )
    val expected = Seq(
      WordCount(Some("i"), Some(10)),
      WordCount(Some("thou"), Some(20)),
      WordCount(Some("thy"), Some(30))
    )

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

  it should "fail duplicate mockWildcardTable" in {
    val prefix = "bigquery-public-data.noaa_gsod.gsod20"
    val suffix = "20"
    val table = "bigquery-public-data:noaa_gsod.gsod2020"
    val mbq = MockBigQuery()
    mbq.mockWildcardTable(prefix, suffix)
    the[IllegalArgumentException] thrownBy {
      mbq.mockWildcardTable(prefix, suffix)
    } should have message s"requirement failed: Table $table already registered for mocking"
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
