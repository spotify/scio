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

import java.util.UUID

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.client.BigQuery
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord
import org.apache.beam.sdk.testing.PAssert
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

object BeamSchemaIT {
  final case class Shakespeare(word: String, word_count: Long, corpus: String, corpus_date: Long)

  val AlienExpected: Set[Shakespeare] = Set(
    Shakespeare("alien", 1, "sonnets", 0),
    Shakespeare("alien", 1, "merchantofvenice", 1596),
    Shakespeare("alien", 1, "1kinghenryiv", 1597)
  )
}

final class BeamSchemaIT extends AnyFlatSpec with Matchers {
  import BeamSchemaIT._

  "typedBigQueryTable" should "read" in {
    val args = Array(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-eu/temp"
    )
    val (sc, _) = ContextAndArgs(args)
    val table = Table.Spec("bigquery-public-data:samples.shakespeare")

    val p = sc
      .typedBigQueryTable[Shakespeare](table)
      .filter(_.word == "alien")
      .internal

    PAssert.that(p).containsInAnyOrder(AlienExpected.asJava)
    sc.run()
  }

  it should "read with parseFn" in {
    val args = Array(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-eu/temp"
    )
    val (sc, _) = ContextAndArgs(args)
    val table = Table.Spec("bigquery-public-data:samples.shakespeare")

    val parseFn: SchemaAndRecord => Shakespeare = sr => {
      val record = sr.getRecord
      Shakespeare(
        record.get("word").toString,
        record.get("word_count").asInstanceOf[Long],
        record.get("corpus").toString,
        record.get("corpus_date").asInstanceOf[Long]
      )
    }

    val p = sc
      .typedBigQueryTable[Shakespeare](table, parseFn)
      .filter(_.word == "alien")
      .internal

    PAssert.that(p).containsInAnyOrder(AlienExpected.asJava)
    sc.run()
  }

  "saveAsBigQuery" should "write" in {
    val args = Array(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-eu/temp"
    )
    val (sc, _) = ContextAndArgs(args)
    val uuid = UUID.randomUUID().toString.replaceAll("-", "")
    val table = Table.Spec(s"data-integration-test:schema_it.shakespeare_schema_$uuid")

    val closedTap = sc
      .parallelize(AlienExpected)
      .saveAsBigQueryTable(table, writeDisposition = WriteDisposition.WRITE_TRUNCATE)
    val result = sc.run().waitUntilDone()

    val (readContext, _) = ContextAndArgs(args)
    val p = result.tap(closedTap).open(readContext).internal
    PAssert.that(p).containsInAnyOrder(AlienExpected.asJava)
    readContext.run()

    BigQuery.defaultInstance().tables.delete(table.ref)
  }
}
