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

package com.spotify.scio.bigquery.types

import com.spotify.scio.bigquery.BigQueryClient
import org.scalatest.{Assertion, FlatSpec, Matchers}

import scala.annotation.StaticAnnotation
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object BigQueryTypeIT {
  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] WHERE word = 'Romeo'")
  class LegacyT

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE word = 'Romeo'")
  class SqlT

  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class FromTableT

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM [data-integration-test:partition_a.table_%s]", "$LATEST")
  class LegacyLatestT

  @BigQueryType.fromQuery(
    "SELECT word, word_count FROM `data-integration-test.partition_a.table_%s`", "$LATEST")
  class SqlLatestT

  @BigQueryType.fromTable("data-integration-test:partition_a.table_%s", "$LATEST")
  class FromTableLatestT

  @BigQueryType.toTable
  case class ToTableT(word: String, word_count: Int)

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

  // run this to re-populate tables used for this test and BigQueryPartitionUtilIT
  def main(args: Array[String]): Unit = {
    val bq = BigQueryClient.defaultInstance()
    val data = List(ToTableT("a", 1), ToTableT("b", 2))
    bq.writeTypedRows("data-integration-test:partition_a.table_20170101", data)
    bq.writeTypedRows("data-integration-test:partition_a.table_20170102", data)
    bq.writeTypedRows("data-integration-test:partition_a.table_20170103", data)
    bq.writeTypedRows("data-integration-test:partition_b.table_20170101", data)
    bq.writeTypedRows("data-integration-test:partition_b.table_20170102", data)
    bq.writeTypedRows("data-integration-test:partition_c.table_20170104", data)
  }
}

class BigQueryTypeIT extends FlatSpec with Matchers {

  import BigQueryTypeIT._

  val bq = BigQueryClient.defaultInstance()

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
    val rows = bq.getQueryRows(legacyQuery).toList
    val typed = Seq(LegacyT("Romeo", 117L))
    rows.map(bqt.fromTableRow) shouldBe typed
    typed.map(bqt.toTableRow).map(bqt.fromTableRow) shouldBe typed
  }

  it should "round trip rows with SQL syntax" in {
    val bqt = BigQueryType[SqlT]
    val rows = bq.getQueryRows(sqlQuery).toList
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
    val types = typeOf[T]
      .typeSymbol
      .annotations
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
