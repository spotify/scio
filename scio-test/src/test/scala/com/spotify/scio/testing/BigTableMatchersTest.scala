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

package com.spotify.scio.testing

import com.google.bigtable.v2.Mutation.{MutationCase, SetCell}
import com.google.bigtable.v2._
import com.google.protobuf.ByteString

// scalastyle:off no.whitespace.before.left.bracket
class BigTableMatchersTest extends PipelineSpec with BigTableMatchers {
  private lazy val key1 = ByteString.copyFromUtf8("k1")
  private lazy val key2 = ByteString.copyFromUtf8("k2")
  private lazy val emptyCell = Seq(Mutation.newBuilder().build())

  "BigTableMatchers" should "support containRowKeys" in {
    val tableData: Seq[BTRow] = Seq(
      (key1, emptyCell),
      (key2, emptyCell)
    )

    // should cases
    runWithContext {
      _.parallelize(tableData) should containRowKeys(key1, key2)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) should containRowKeys (key1)
      }
    }

    // should not cases
    runWithContext {
      _.parallelize(tableData) shouldNot containRowKeys(key1)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) shouldNot containRowKeys(key1, key2)
      }
    }
  }

  private lazy val columnFamily1 = "cf1"
  private lazy val columnFamily2 = "cf2"

  it should "support containColumnFamilies" in {
    val cell1 = Mutation.newBuilder()
      .setSetCell(SetCell.newBuilder()
      .setFamilyName(columnFamily1))
      .build()

    val cell2 = Mutation.newBuilder()
      .setSetCell(SetCell.newBuilder()
        .setFamilyName(columnFamily2))
      .build()

    val tableData: Seq[BTRow] = Seq(
      (key1, Seq(cell1)),
      (key2, Seq(cell2))
    )

    // should cases
    runWithContext {
      _.parallelize(tableData) should containColumnFamilies(columnFamily1, columnFamily2)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) should containColumnFamilies (columnFamily1)
      }
    }

    // should not cases
    runWithContext {
      _.parallelize(tableData) shouldNot containColumnFamilies(columnFamily1)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) shouldNot containColumnFamilies (columnFamily1, columnFamily2)
      }
    }
  }

  val cellValue1 = "cv1"
  val cellValue2 = "cv2"

  it should "support containSetCellValue" in {
    val cell1 = Mutation.newBuilder()
      .setSetCell(SetCell.newBuilder()
        .setFamilyName(columnFamily1)
        .setColumnQualifier(columnFamily1)
        .setValue(ByteString.copyFromUtf8(cellValue1))
      ).build()

    val cell2 = Mutation.newBuilder()
      .setSetCell(SetCell.newBuilder()
        .setFamilyName(columnFamily2)
        .setColumnQualifier(columnFamily2)
        .setValue(ByteString.copyFromUtf8(cellValue2))
      ).build()

    val tableData: Seq[BTRow] = Seq(
      (key1, Seq(cell1, cell2))
    )

    // should cases
    runWithContext { sc =>
      sc.parallelize(tableData) should containSetCellValue(key1, columnFamily1, cellValue1)
      sc.parallelize(tableData) should containSetCellValue(key1, columnFamily2, cellValue2)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) should containSetCellValue(key2, columnFamily1, cellValue1)
      }
    }

    // should not cases
    runWithContext {
      _.parallelize(tableData) shouldNot containSetCellValue(key2, columnFamily1, cellValue1)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) shouldNot containSetCellValue(key1, columnFamily1, cellValue1)
      }
    }
  }

  it should "support containCellMutationCase" in {
    val cell = Mutation.newBuilder()
      .setDeleteFromRow(Mutation.DeleteFromRow.newBuilder().build())
      .build()

    val tableData: Seq[BTRow] = Seq(
      (key1, Seq(cell))
    )

    // should cases
    runWithContext {
      _.parallelize(tableData) should containCellMutationCase(key1, MutationCase.DELETE_FROM_ROW)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) should containCellMutationCase(key1, MutationCase.SET_CELL)
      }
    }

    // should not cases
    runWithContext {
      _.parallelize(tableData) shouldNot containCellMutationCase(key1, MutationCase.SET_CELL)
    }

    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(tableData) shouldNot containCellMutationCase(key1,
          MutationCase.DELETE_FROM_ROW)
      }
    }
  }
}
// scalastyle:on no.whitespace.before.left.bracket
