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

package com.spotify.scio.testing

import com.google.bigtable.v2.Mutation
import com.google.bigtable.v2.Mutation.MutationCase
import com.google.protobuf.ByteString
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder

import org.scalatest.matchers.{MatchResult, Matcher}

import scala.reflect.ClassTag

/**
 * Trait with ScalaTest [[org.scalatest.matchers.Matcher Matcher]]s for
 * [[com.spotify.scio.values.SCollection SCollection]]s specific to Bigtable output.
 */
trait BigtableMatchers extends SCollectionMatchers {

  import EqInstances._

  type BTRow = (ByteString, Iterable[Mutation])
  type BTCollection = SCollection[BTRow]

  // Needed because scalac is an idiot
  implicit def btCollCoder: Coder[BTRow] =
    Coder.gen[(ByteString, Iterable[Mutation])]

  /** Provide an implicit BT serializer for common cell value type String. */
  implicit def stringBTSerializer(s: String): ByteString =
    ByteString.copyFromUtf8(s)

  /** Check that the BT collection contains only the given keys, in any order. */
  def containRowKeys(expectedKeys: ByteString*): Matcher[BTCollection] =
    new Matcher[BTCollection] {
      override def apply(left: BTCollection): MatchResult =
        containInAnyOrder(expectedKeys).apply(left.keys)
    }

  /**
   * Check that the BT collection contains only the given column families, unique, in any order.
   */
  def containColumnFamilies(expectedCFs: String*): Matcher[BTCollection] =
    new Matcher[BTCollection] {
      override def apply(left: BTCollection): MatchResult = {
        val foundCFs = left.flatMap {
          case (_, cells) =>
            cells.map(_.getSetCell.getFamilyName)
        }

        containInAnyOrder(expectedCFs).apply(foundCFs.distinct)
      }
    }

  /**
   * Check that the BT collection contains a cell with the given row key, column family, and
   * deserialized cell value. Column qualifier defaults to the same as column family.
   */
  def containSetCellValue[V: ClassTag](key: ByteString, cf: String, value: V)(
    implicit ser: V => ByteString
  ): Matcher[BTCollection] =
    containSetCellValue(key, cf, cf, value)

  /**
   * Check that the BT collection contains a cell with the given row key, column family, column
   * qualifier, and deserialized cell value.
   * @param key Row key the cell should be in
   * @param cf Column family the cell should have
   * @param cq Column qualifier the cell should have
   * @param value Deserialized value of the set cell
   * @param ser Serializer to convert value type V to ByteString for BT format
   * @tparam V Class of expected value
   */
  def containSetCellValue[V: ClassTag](key: ByteString, cf: String, cq: String, value: V)(
    implicit ser: V => ByteString
  ): Matcher[BTCollection] =
    new Matcher[BTCollection] {
      override def apply(left: BTCollection): MatchResult = {
        val flattenedRows = left.flatMap {
          case (rowKey, rowValue) =>
            rowValue.map { cell =>
              (
                rowKey,
                cell.getSetCell.getFamilyName,
                cell.getSetCell.getColumnQualifier,
                cell.getSetCell.getValue
              )
            }
        }

        containValue(
          (
            key,
            cf,
            ByteString.copyFromUtf8(cq),
            ser.apply(value)
          )
        ).apply(flattenedRows)
      }
    }

  /**
   * Check that the BT collection contains a cell with the given row key and enumerated
   * MutationCase, making no assumptions about the contents of the rest of the collection.
   */
  def containCellMutationCase[V: ClassTag](
    key: ByteString,
    mutation: MutationCase
  ): Matcher[BTCollection] =
    new Matcher[BTCollection] {
      override def apply(left: BTCollection): MatchResult = {
        val flattenedRows = left.flatMap {
          case (rowKey, rowValue) =>
            rowValue.map(cell => (rowKey, cell.getMutationCase.toString))
        }

        containValue((key, mutation.toString)).apply(flattenedRows)
      }
    }
}
