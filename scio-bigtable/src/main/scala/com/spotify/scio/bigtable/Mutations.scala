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

import java.util.concurrent.TimeUnit

import com.google.bigtable.v2.Mutation.{DeleteFromColumn, DeleteFromFamily, DeleteFromRow, SetCell}
import com.google.bigtable.v2._
import com.google.protobuf.ByteString

/** Helper methods for `Mutation`. */
object Mutations {
  /** New `SetCell` mutation using the current timestamp. */
  def newSetCell(familyName: String, columnQualifier: ByteString, value: ByteString): Mutation =
    Mutation
      .newBuilder()
      .setSetCell(
        SetCell
          .newBuilder()
          .setFamilyName(familyName)
          .setColumnQualifier(columnQualifier)
          .setValue(value)
          .setTimestampMicros(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()))
      )
      .build()

  /** New `SetCell` mutation. */
  def newSetCell(
    familyName: String,
    columnQualifier: ByteString,
    value: ByteString,
    timestampMicros: Long
  ): Mutation =
    Mutation
      .newBuilder()
      .setSetCell(
        SetCell
          .newBuilder()
          .setFamilyName(familyName)
          .setColumnQualifier(columnQualifier)
          .setValue(value)
          .setTimestampMicros(timestampMicros)
      )
      .build()

  /** New `DeleteFromColumn` mutation. */
  def newDeleteFromColumn(
    familyName: String,
    columnQualifier: ByteString,
    startTimestampMicros: Long,
    endTimestampMicros: Long
  ): Mutation =
    Mutation
      .newBuilder()
      .setDeleteFromColumn(
        DeleteFromColumn
          .newBuilder()
          .setFamilyName(familyName)
          .setColumnQualifier(columnQualifier)
          .setTimeRange(
            TimestampRange
              .newBuilder()
              .setStartTimestampMicros(startTimestampMicros)
              .setEndTimestampMicros(endTimestampMicros)
          )
      )
      .build()

  /** New `DeleteFromFamily` mutation. */
  def newDeleteFromFamily(familyName: String): Mutation =
    Mutation
      .newBuilder()
      .setDeleteFromFamily(
        DeleteFromFamily
          .newBuilder()
          .setFamilyName(familyName)
      )
      .build()

  /** New `DeleteFromRow` mutation. */
  def newDeleteFromRow: Mutation =
    Mutation
      .newBuilder()
      .setDeleteFromRow(DeleteFromRow.getDefaultInstance)
      .build()
}
