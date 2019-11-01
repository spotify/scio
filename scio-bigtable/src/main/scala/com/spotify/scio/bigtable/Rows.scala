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

package com.spotify.scio.bigtable

import com.google.bigtable.v2.{Cell, Column, Family, Row}
import com.google.protobuf.ByteString

/** Helper methods for `Row`. */
object Rows {
  private def newCell(value: ByteString): Cell =
    Cell.newBuilder().setValue(value).build()

  private def newCell(value: ByteString, timestampMicros: Long): Cell =
    Cell
      .newBuilder()
      .setValue(value)
      .setTimestampMicros(timestampMicros)
      .build()

  private def newRow(
    key: ByteString,
    familyName: String,
    columnQualifier: ByteString,
    cell: Cell
  ): Row =
    Row
      .newBuilder()
      .setKey(key)
      .addFamilies(
        Family
          .newBuilder()
          .setName(familyName)
          .addColumns(
            Column
              .newBuilder()
              .setQualifier(columnQualifier)
              .addCells(cell)
          )
      )
      .build()

  /** New `Row` with timestamp default to 0. */
  def newRow(
    key: ByteString,
    familyName: String,
    columnQualifier: ByteString,
    value: ByteString
  ): Row =
    newRow(key, familyName, columnQualifier, newCell(value))

  /** New `Row`. */
  def newRow(
    key: ByteString,
    familyName: String,
    columnQualifier: ByteString,
    value: ByteString,
    timestampMicros: Long
  ): Row =
    newRow(key, familyName, columnQualifier, newCell(value, timestampMicros))
}
