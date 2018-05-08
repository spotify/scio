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

package com.spotify.scio.bigquery.testing

import com.spotify.scio._
import com.spotify.scio.testing._
import com.spotify.scio.bigquery._

// scalastyle:off file.size.limit

object BigQueryJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.bigQueryTable(args("input"))
      .saveAsBigQuery(args("output"))
    sc.close()
  }
}

object TableRowJsonJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.tableRowJsonFile(args("input"))
      .saveAsTableRowJsonFile(args("output"))
    sc.close()
  }
}

// scalastyle:off no.whitespace.before.left.bracket
class JobTestTest extends PipelineSpec {

  def newTableRow(i: Int): TableRow = TableRow("int_field" -> i)

  def testBigQuery(xs: Seq[TableRow]): Unit = {
    JobTest[BigQueryJob.type]
      .args("--input=table.in", "--output=table.out")
      .input(BigQueryIO("table.in"), (1 to 3).map(newTableRow))
      .output(BigQueryIO[TableRow]("table.out"))(_ should containInAnyOrder (xs))
      .run()
  }

  "JobTest" should "pass correct BigQueryJob" in {
    testBigQuery((1 to 3).map(newTableRow))
  }

  it should "fail incorrect BigQueryJob" in {
    an [AssertionError] should be thrownBy { testBigQuery((1 to 2).map(newTableRow)) }
    an [AssertionError] should be thrownBy { testBigQuery((1 to 4).map(newTableRow)) }
  }

  def testTableRowJson(xs: Seq[TableRow]): Unit = {
    JobTest[TableRowJsonJob.type]
      .args("--input=in.json", "--output=out.json")
      .input(TableRowJsonIO("in.json"), (1 to 3).map(newTableRow))
      .output(TableRowJsonIO("out.json"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct TableRowJsonIO" in {
    testTableRowJson((1 to 3).map(newTableRow))
  }

  it should "fail incorrect TableRowJsonIO" in {
    an [AssertionError] should be thrownBy { testTableRowJson((1 to 2).map(newTableRow)) }
    an [AssertionError] should be thrownBy { testTableRowJson((1 to 4).map(newTableRow)) }
  }

}
// scalastyle:on no.whitespace.before.left.bracket
// scalastyle:on file.size.limit
