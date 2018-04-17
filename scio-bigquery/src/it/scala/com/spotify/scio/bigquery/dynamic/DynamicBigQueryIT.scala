/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.bigquery.dynamic

import java.util.UUID

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio._
import com.spotify.scio.bigquery._
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest._

object DynamicBigQueryIT {
  val projectId = "data-integration-test"
  val datasetId = "bigquery_dynamic_it"

  def tableRef(prefix: String, name: String): TableReference =
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(datasetId)
      .setTableId(prefix + "_" + name)

  @BigQueryType.toTable
  case class Record(key: Int, value: String)

  def newRecord(x: Int): Record = Record(x, x.toString)
}

class DynamicBigQueryIT extends FlatSpec with Matchers {

  import DynamicBigQueryIT._

  private val bq = BigQueryClient.defaultInstance()

  private val options = PipelineOptionsFactory
    .fromArgs(
      s"--project=$projectId",
      "--tempLocation=gs://data-integration-test-us/temp")
    .create()

  "Dynamic BigQuery" should "support typed output" in {
    val prefix = UUID.randomUUID().toString.replaceAll("-", "")
    val sc = ScioContext(options)
    sc.parallelize(1 to 10)
      .map(newRecord)
      .saveAsTypedBigQuery(WRITE_EMPTY, CREATE_IF_NEEDED) { v =>
        val mod = v.getValue.key % 2
        new TableDestination(tableRef(prefix, mod.toString), s"key % 10 == $mod")
      }
    sc.close()

    val expected = (1 to 10).map(newRecord).toSet
    val rows0 = bq.getTypedRows[Record](tableRef(prefix, "0").asTableSpec).toSet
    val rows1 = bq.getTypedRows[Record](tableRef(prefix, "1").asTableSpec).toSet
    rows0 shouldBe expected.filter(_.key % 2 == 0)
    rows1 shouldBe expected.filter(_.key % 2 == 1)
  }

  it should "support TableRow output" in {
    val prefix = UUID.randomUUID().toString.replaceAll("-", "")
    val sc = ScioContext(options)
    sc.parallelize(1 to 10)
      .map(newRecord)
      .map(Record.toTableRow)
      .saveAsBigQuery(Record.schema, WRITE_EMPTY, CREATE_IF_NEEDED) { v =>
        val mod = v.getValue.get("key").toString.toInt % 2
        new TableDestination(tableRef(prefix, mod.toString), s"key % 10 == $mod")
      }
    sc.close()

    val expected = (1 to 10).map(newRecord).toSet
    val rows0 = bq.getTypedRows[Record](tableRef(prefix, "0").asTableSpec).toSet
    val rows1 = bq.getTypedRows[Record](tableRef(prefix, "1").asTableSpec).toSet
    rows0 shouldBe expected.filter(_.key % 2 == 0)
    rows1 shouldBe expected.filter(_.key % 2 == 1)
  }

}
