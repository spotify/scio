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

package com.spotify.scio.bigquery.types

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.{Query, Table}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.StaticAnnotation
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._

object BigQueryTypeIT {
  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] WHERE word = 'Romeo'"
  )
  class LegacyT

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word = 'Romeo'"
  )
  class SqlT

  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class FromTableT

  @BigQueryType.toTable
  case class ToTableT(word: String, word_count: Int)

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM [data-integration-test:partition_a.table_%s]",
    "$LATEST"
  )
  class LegacyLatestT

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM `data-integration-test.partition_a.table_%s`",
    "$LATEST"
  )
  class SqlLatestT

  @BigQueryType.fromQuery(
    """
      |SELECT word, word_count
      |FROM `data-integration-test.partition_a.table_%s`
      |WHERE word_count > %3$d and word != '%%'
      |LIMIT %d
    """.stripMargin,
    "$LATEST",
    1,
    1
  )
  class SqlLatestTWithMultiArgs

  @BigQueryType.fromTable("data-integration-test:partition_a.table_%s", "$LATEST")
  class FromTableLatestT

  class Annotation1 extends StaticAnnotation
  class Annotation2 extends StaticAnnotation

  @Annotation1
  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  @Annotation2
  class ShakespeareWithSurroundingAnnotations

  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  @Annotation1
  @Annotation2
  class ShakespeareWithSequentialAnnotations
}

// scio-test/it:runMain com.spotify.scio.PopulateTestData to re-populate data for integration tests
class BigQueryTypeIT extends AnyFlatSpec with Matchers {
  import BigQueryTypeIT._

  val bq: BigQuery = BigQuery.defaultInstance()

  val legacyQuery =
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] WHERE word = 'Romeo'"
  val sqlQuery =
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word = 'Romeo'"
  val legacyLatestQuery =
    "SELECT word, word_count FROM [data-integration-test:partition_a.table_%s]"
  val sqlLatestQuery =
    "SELECT word, word_count FROM `data-integration-test.partition_a.table_%s`"

  "fromQuery" should "work with legacy syntax" in {
    val bqt = BigQueryType[LegacyT]
    bqt.isQuery shouldBe true
    bqt.isTable shouldBe false
    bqt.query shouldBe Some(legacyQuery)
    bqt.table shouldBe None
    val fields = bqt.schema.getFields.asScala
    fields.size shouldBe 2
    fields.map(_.getName) shouldBe Seq("word", "word_count")
    fields.map(_.getType) shouldBe Seq("STRING", "INTEGER")
    fields.map(_.getMode) shouldBe Seq("REQUIRED", "REQUIRED")
  }

  it should "work with SQL syntax" in {
    val bqt = BigQueryType[SqlT]
    bqt.isQuery shouldBe true
    bqt.isTable shouldBe false
    bqt.query shouldBe Some(sqlQuery)
    bqt.table shouldBe None
    val fields = bqt.schema.getFields.asScala
    fields.size shouldBe 2
    fields.map(_.getName) shouldBe Seq("word", "word_count")
    fields.map(_.getType) shouldBe Seq("STRING", "INTEGER")
    fields.map(_.getMode) shouldBe Seq("NULLABLE", "NULLABLE")
  }

  it should "round trip rows with legacy syntax" in {
    val bqt = BigQueryType[LegacyT]
    val rows = bq.query.rows(legacyQuery).toList
    val typed = Seq(LegacyT("Romeo", 117L))
    rows.map(bqt.fromTableRow) shouldBe typed
    typed.map(bqt.toTableRow).map(bqt.fromTableRow) shouldBe typed
  }

  it should "round trip rows with SQL syntax" in {
    val bqt = BigQueryType[SqlT]
    val rows = bq.query.rows(sqlQuery).toList
    val typed = Seq(SqlT(Some("Romeo"), Some(117L)))
    rows.map(bqt.fromTableRow) shouldBe typed
    typed.map(bqt.toTableRow).map(bqt.fromTableRow) shouldBe typed
  }

  it should "work with legacy syntax with $LATEST" in {
    BigQueryType[LegacyLatestT].query shouldBe Some(legacyLatestQuery)
  }

  it should "work with SQL syntax with $LATEST" in {
    BigQueryType[SqlLatestT].query shouldBe Some(sqlLatestQuery)
  }

  it should "have query fn" in {
    """LegacyLatestT.query("TABLE")""" should compile
    """SqlLatestT.query("TABLE")""" should compile
  }

  it should "have query fn with only 1 argument" in {
    """LegacyLatestT.query("TABLE", 1)""" shouldNot typeCheck
    """SqlLatestT.query("TABLE", 1)""" shouldNot typeCheck
  }

  it should "have query fn with multiple arguments" in {
    """SqlLatestTWithMultiArgs.query("TABLE", 1, 1)""" should compile
    """SqlLatestTWithMultiArgs.query(1, "TABLE", 1)""" shouldNot typeCheck
  }

  it should "format query" in {
    LegacyLatestT.query("TABLE") shouldBe legacyLatestQuery.format("TABLE")
    SqlLatestT.query("TABLE") shouldBe sqlLatestQuery.format("TABLE")
  }

  it should "format and return query as source" in {
    LegacyLatestT.queryAsSource("TABLE") shouldBe Query(legacyLatestQuery.format("TABLE"))
    LegacyLatestT.queryAsSource("$LATEST").latest(bq) shouldBe Query(
      legacyLatestQuery.format("$LATEST")
    ).latest(bq)
    SqlLatestT.queryAsSource("TABLE") shouldBe Query(sqlLatestQuery.format("TABLE"))
    SqlLatestT.queryAsSource("$LATEST").latest(bq) shouldBe Query(sqlLatestQuery.format("$LATEST"))
      .latest(bq)
  }

  it should "resolve latest Table" in {
    val tableReference = new TableReference
    tableReference.setProjectId("data-integration-test")
    tableReference.setDatasetId("partition_a")
    tableReference.setTableId("table_$LATEST")
    Table.Ref(tableReference).latest().ref.getTableId shouldBe "table_20170103"

    Table
      .Spec("data-integration-test:partition_a.table_$LATEST")
      .latest()
      .ref
      .getTableId shouldBe "table_20170103"
  }

  it should "type check annotation arguments" in {
    """
      |  @BigQueryType.fromQuery(
      |    "SELECT word, word_count FROM `data-integration-test.partition_a.table_%s` LIMIT %d",
      |    "$LATEST",
      |    "1")
      |  class WrongFormatSupplied
    """.stripMargin shouldNot compile
  }

  "fromTable" should "work" in {
    val bqt = BigQueryType[FromTableT]
    bqt.isQuery shouldBe false
    bqt.isTable shouldBe true
    bqt.query shouldBe None
    bqt.table shouldBe Some("bigquery-public-data:samples.shakespeare")
    val fields = bqt.schema.getFields.asScala
    fields.size shouldBe 4
    fields.map(_.getName) shouldBe Seq("word", "word_count", "corpus", "corpus_date")
    fields.map(_.getType) shouldBe Seq("STRING", "INTEGER", "STRING", "INTEGER")
    fields.map(_.getMode) shouldBe Seq("REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED")
  }

  it should "work with $LATEST" in {
    BigQueryType[FromTableLatestT].table shouldBe Some("data-integration-test:partition_a.table_%s")
  }

  def containsAllAnnotTypes[T: TypeTag]: Assertion = {
    val types = typeOf[T].typeSymbol.annotations
      .map(_.tree.tpe)
    Seq(typeOf[Annotation1], typeOf[Annotation2])
      .forall(lt => types.exists(rt => lt =:= rt)) shouldBe true
  }

  it should "preserve surrounding user defined annotations" in {
    containsAllAnnotTypes[ShakespeareWithSurroundingAnnotations]
  }

  it should "preserve sequential user defined annotations" in {
    containsAllAnnotTypes[ShakespeareWithSequentialAnnotations]
  }

  "toTable" should "work" in {
    val bqt = BigQueryType[ToTableT]
    bqt.isQuery shouldBe false
    bqt.isTable shouldBe false
    bqt.query shouldBe None
    bqt.table shouldBe None
    val fields = bqt.schema.getFields.asScala
    fields.size shouldBe 2
    fields.map(_.getName) shouldBe Seq("word", "word_count")
    fields.map(_.getType) shouldBe Seq("STRING", "INTEGER")
    fields.map(_.getMode) shouldBe Seq("REQUIRED", "REQUIRED")
  }
}
