/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.scio.examples.extra

import com.google.bigtable.v1.Mutation.SetCell
import com.google.bigtable.v1._
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._

object BigtableV2Util {

  trait Coder[T] {
    def encode(x: T): ByteString
    def decode(x: ByteString): T
  }

  implicit val stringCoder = new Coder[String] {
    override def encode(x: String): ByteString = ByteString.copyFromUtf8(x)
    override def decode(x: ByteString): String = x.toStringUtf8
  }

  implicit val bytesCoder = new Coder[Array[Byte]] {
    override def encode(x: Array[Byte]): ByteString = ByteString.copyFrom(x)
    override def decode(x: ByteString): Array[Byte] = x.toByteArray
  }

  // =======================================================================
  // Row creation
  // =======================================================================

  /** Create a simple [[Row]] instance. */
  def simpleRow[K: Coder, F: Coder, C: Coder, V: Coder]
  (rowKey: K, familyName: F, columnQualifier: C, cellValue: V): Row =
    row(rowKey, family(familyName, column(columnQualifier, cellValue)))

  def row[K](key: K, families: Family*)(implicit kc: Coder[K]): Row =
    Row.newBuilder()
      .setKey(kc.encode(key))
      .addAllFamilies(families.asJava)
      .build()

  def family[F](familyName: F, columns: Column*)(implicit fc: Coder[F]): Family =
    Family.newBuilder()
      .setNameBytes(fc.encode(familyName))
      .addAllColumns(columns.asJava)
      .build()

  def column[C , V](columnQualifier: C, values: V*)(implicit cc: Coder[C], vc: Coder[V]): Column =
    Column.newBuilder()
      .setQualifier(cc.encode(columnQualifier))
      .addAllCells(values.map(cell(_)).asJava)
      .build()

  def cell[V](value: V)(implicit vc: Coder[V]): Cell =
    Cell.newBuilder().setValue(vc.encode(value)).build()

  // =======================================================================
  // Row access
  // =======================================================================

  def simpleGetCells[F, C](row: Row, familyName: F, columnQualifier: C)
                          (implicit fc: Coder[F], cc: Coder[C]): Iterable[ByteString] = {
    row
      .getFamiliesList.asScala
      .filter(f => fc.decode(f.getNameBytes) == familyName)
      .flatMap { _
        .getColumnsList.asScala
        .filter(c => cc.decode(c.getQualifier) == columnQualifier)
        .flatMap(c => c.getCellsList.asScala.map(_.getValue))
      }
  }

  // =======================================================================
  // Mutation
  // =======================================================================

  /** Create a simple (row key, mutations) tuple. */
  def simpleSetCell[K, F, C, V](key: K, familyName: F, columnQualifier: C, value: V)
                               (implicit kc: Coder[K], fc: Coder[F], cc: Coder[C], vc: Coder[V])
  : (ByteString, Iterable[Mutation]) =
    (kc.encode(key), Iterable(setCell(familyName, columnQualifier, value)))

  def setCell[F, C, V](familyName: F, columnQualifier: C, value: V)
                      (implicit fc: Coder[F], cc: Coder[C], vc: Coder[V]): Mutation =
    Mutation.newBuilder().setSetCell(
      SetCell.newBuilder()
        .setFamilyNameBytes(fc.encode(familyName))
        .setColumnQualifier(cc.encode(columnQualifier))
        .setValue(vc.encode(value))
        .setTimestampMicros(-1)
    ).build()

}
