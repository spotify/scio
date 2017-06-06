/*
 * Copyright 2017 Spotify AB.
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

import org.scalatest._

class BigQueryPartitionUtilIT extends FlatSpec with Matchers {

  val bq = BigQueryClient.defaultInstance()

  "latestQuery" should "work with legacy syntax" in {
    val input =
      """
        |SELECT *
        |FROM [data-integration-test:samples_us.shakespeare]
        |JOIN [data-integration-test:partition_a.table_$LATEST]
        |JOIN [data-integration-test:partition_b.table_$LATEST]
        |WHERE x = 0
      """.stripMargin
    val expected = input.replace("$LATEST", "20170102")
    BigQueryPartitionUtil.latestQuery(bq, input) shouldBe expected
  }

  it should "work with SQL syntax" in {
    val input =
      """
        |SELECT *
        |FROM `data-integration-test.samples_us.shakespeare`
        |JOIN `data-integration-test.partition_a.table_$LATEST`
        |JOIN `data-integration-test.partition_b.table_$LATEST`
        |WHERE x = 0
      """.stripMargin
    val expected = input.replace("$LATEST", "20170102")
    BigQueryPartitionUtil.latestQuery(bq, input) shouldBe expected
  }

  it should "work with legacy syntax without $LATEST" in {
    val input = "SELECT * FROM [data-integration-test:samples_us.shakespeare]"
    BigQueryPartitionUtil.latestQuery(bq, input) shouldBe input
  }

  it should "work with SQL syntax without $LATEST" in {
    val input = "SELECT * FROM `data-integration-test.samples_us.shakespeare`"
    BigQueryPartitionUtil.latestQuery(bq, input) shouldBe input
  }

  it should "fail legacy syntax without latest common partition" in {
    val input =
      """
        |SELECT *
        |FROM [data-integration-test:samples_us.shakespeare]
        |JOIN [data-integration-test:partition_a.table_$LATEST]
        |JOIN [data-integration-test:partition_b.table_$LATEST]
        |JOIN [data-integration-test:partition_c.table_$LATEST]
        |WHERE x = 0
      """.stripMargin
    val msg = "requirement failed: Cannot find latest common partition for " +
      "[data-integration-test:partition_a.table_$LATEST], " +
      "[data-integration-test:partition_b.table_$LATEST], " +
      "[data-integration-test:partition_c.table_$LATEST]"
    // scalastyle:off no.whitespace.before.left.bracket
    the [IllegalArgumentException] thrownBy {
      BigQueryPartitionUtil.latestQuery(bq, input)
    } should have message msg
    // scalastyle:on no.whitespace.before.left.bracket
  }

  it should "fail SQL syntax without latest common partition" in {
    val input =
      """
        |SELECT *
        |FROM `data-integration-test.samples_us.shakespeare`
        |JOIN `data-integration-test.partition_a.table_$LATEST`
        |JOIN `data-integration-test.partition_b.table_$LATEST`
        |JOIN `data-integration-test.partition_c.table_$LATEST`
        |WHERE x = 0
      """.stripMargin
    val msg = "requirement failed: Cannot find latest common partition for " +
      "`data-integration-test.partition_a.table_$LATEST`, " +
      "`data-integration-test.partition_b.table_$LATEST`, " +
      "`data-integration-test.partition_c.table_$LATEST`"
    // scalastyle:off no.whitespace.before.left.bracket
    the [IllegalArgumentException] thrownBy {
      BigQueryPartitionUtil.latestQuery(bq, input)
    } should have message msg
    // scalastyle:on no.whitespace.before.left.bracket
  }

  "latestTable" should "work" in {
    val input = "data-integration-test:partition_a.table_$LATEST"
    val expected = input.replace("$LATEST", "20170103")
    BigQueryPartitionUtil.latestTable(bq, input) shouldBe expected
  }

  it should "work without $LATEST" in {
    val input = "data-integration-test:samples_us.shakespeare"
    BigQueryPartitionUtil.latestTable(bq, input) shouldBe input
  }

  it should "fail table specification without latest partition" in {
    val input = "data-integration-test:samples_us.shakespeare_$LATEST"
    val msg = s"requirement failed: Cannot find latest partition for $input"
    // scalastyle:off no.whitespace.before.left.bracket
    the [IllegalArgumentException] thrownBy {
      BigQueryPartitionUtil.latestTable(bq, input)
    } should have message msg
    // scalastyle:on no.whitespace.before.left.bracket
  }

}
