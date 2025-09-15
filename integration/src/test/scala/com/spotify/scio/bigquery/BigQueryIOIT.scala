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

import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.coders.Coder
import com.spotify.scio.avro._
import org.apache.beam.sdk.options._
import com.spotify.scio.testing._
import com.spotify.scio.testing.util.ItUtils
import org.apache.avro.generic.GenericRecord

object BigQueryIOIT {
  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class ShakespeareFromTable

  @BigQueryType.fromQuery(
    """SELECT word, word_count
      |FROM `bigquery-public-data.samples.shakespeare`
      |WHERE corpus = 'kinglear'
      |ORDER BY word_count DESC
      |LIMIT 5""".stripMargin
  )
  class ShakespeareFromQuery
}

class BigQueryIOIT extends PipelineSpec {
  import BigQueryIOIT._
  import ItUtils.project

  val tempLocation: String = ItUtils.gcpTempLocation("bigquery-it")
  val options: PipelineOptions = PipelineOptionsFactory
    .fromArgs(s"--project=$project", s"--tempLocation=$tempLocation")
    .create()

  val kinglearTop5: Seq[(String, Long)] = Seq(
    "the" -> 786L,
    "I" -> 622L,
    "and" -> 594L,
    "of" -> 447L,
    "to" -> 438L
  )

  def extractCorpus(r: TableRow): String =
    r.get("corpus").asInstanceOf[String]

  def extractCorpus(r: GenericRecord): String =
    r.get("corpus").asInstanceOf[CharSequence].toString

  def extractWordCount(r: TableRow): (String, Long) = {
    val word = r.get("word").asInstanceOf[String]
    val count = r.get("word_count").asInstanceOf[String].toLong
    word -> count
  }

  def extractWordCount(r: GenericRecord): (String, Long) = {
    val word = r.get("word").asInstanceOf[CharSequence].toString
    val count = r.get("word_count").asInstanceOf[Long]
    word -> count
  }

  "Select" should "read values from a SQL query" in {
    runWithRealContext(options) { sc =>
      val query = Query(ShakespeareFromQuery.queryRaw)
      val scoll = sc.bigQuerySelect(query).map(extractWordCount)

      scoll should containInAnyOrder(kinglearTop5)
    }
  }

  it should "read storage values from a SQL query" in {
    runWithRealContext(options) { sc =>
      val query = Query(ShakespeareFromQuery.queryRaw)
      val scoll = sc
        .bigQueryStorage(query)
        .map(extractWordCount)

      scoll should containInAnyOrder(kinglearTop5)
    }
  }

  it should "read typed values from a SQL query" in {
    runWithRealContext(options) { sc =>
      val scoll = sc
        .typedBigQuery[ShakespeareFromQuery]()
        .flatMap { r =>
          for {
            w <- r.word
            c <- r.word_count
          } yield w -> c
        }

      scoll should containInAnyOrder(kinglearTop5)
    }
  }

  "Table" should "read values from table" in {
    runWithRealContext(options) { sc =>
      val table = Table.Spec(ShakespeareFromTable.table)
      val scoll = sc
        .bigQueryTable(table)
        .filter(r => extractCorpus(r) == "kinglear")
        .map(extractWordCount)
        .top(5)(Ordering.by(_._2))
        .flatten

      scoll should containInAnyOrder(kinglearTop5)
    }
  }

  it should "read avro values from table" in {
    runWithRealContext(options) { sc =>
      // BQ limitation: We can't give an avro reader schema
      implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder
      val table = Table.Spec(ShakespeareFromTable.table)
      val scoll = sc
        .bigQueryTable[GenericRecord](table, Format.GenericRecord)
        .filter(r => extractCorpus(r) == "kinglear")
        .map(extractWordCount)
        .top(5)(Ordering.by(_._2))
        .flatten

      scoll should containInAnyOrder(kinglearTop5)
    }
  }

  it should "read storage values from table" in {
    runWithRealContext(options) { sc =>
      val table = Table.Spec(ShakespeareFromTable.table)
      val scoll = sc
        .bigQueryStorage(table)
        .filter(r => extractCorpus(r) == "kinglear")
        .map(extractWordCount)
        .top(5)(Ordering.by(_._2))
        .flatten

      scoll should containInAnyOrder(kinglearTop5)
    }
  }

  it should "read typed values from table" in {
    runWithRealContext(options) { sc =>
      val scoll = sc
        .typedBigQuery[ShakespeareFromTable]()
        .collect { case r if r.corpus == "kinglear" => r.word -> r.word_count }
        .top(5)(Ordering.by(_._2))
        .flatten

      scoll should containInAnyOrder(kinglearTop5)
    }
  }
}
