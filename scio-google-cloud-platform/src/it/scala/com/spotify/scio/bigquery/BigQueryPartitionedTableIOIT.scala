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

import java.util.UUID

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.testing.util.ItUtils
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BigQueryPartitionedTableIOIT extends AnyFlatSpec with Matchers {
  import BigQueryIOIT._
  import ItUtils.project
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

  private val bq = BigQuery.defaultInstance()
  private val options: PipelineOptions = PipelineOptionsFactory
    .fromArgs(s"--project=$project", s"--tempLocation=$tempLocation")
    .create()

  it should "support typed output" in {
    val prefix = UUID.randomUUID().toString.replaceAll("-", "")
    val sc = ScioContext(options)

    sc.parallelize(1 to 3)
      .map(newRecord)
      .saveAsTypedPartitionedTable(WRITE_EMPTY, CREATE_IF_NEEDED) {
        v: ValueInSingleWindow[Record] =>
          val mod = v.getValue.key % 2
          new TableDestination(tableRef(prefix, mod.toString), s"key % 10 == $mod")
      }
    sc.run()

    val expected = (1 to 3).map(newRecord).toSet
    val rows0 = bq.getTypedRows[Record](tableRef(prefix, "0").asTableSpec).toSet
    val rows1 = bq.getTypedRows[Record](tableRef(prefix, "1").asTableSpec).toSet
    rows0 shouldBe expected.filter(_.key % 2 == 0)
    rows1 shouldBe expected.filter(_.key % 2 == 1)
  }
}
