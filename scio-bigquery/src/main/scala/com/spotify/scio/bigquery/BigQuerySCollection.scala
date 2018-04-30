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

package com.spotify.scio.bigquery

import com.spotify.scio.io._
import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.Compression
import scala.concurrent._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.spotify.scio.values.SCollection
import types.BigQueryType.HasAnnotation

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class BigQuerySCollection[T](@transient val self: SCollection[T]) extends Serializable {

  /**
   * Save this SCollection as a BigQuery table. Note that elements must be of type
   * [[com.google.api.services.bigquery.model.TableRow TableRow]].
   * @group output
   */
  def saveAsBigQuery(table: TableReference, schema: TableSchema,
                     writeDisposition: WriteDisposition,
                     createDisposition: CreateDisposition,
                     tableDescription: String)
                    (implicit ev: T <:< TableRow): Future[Tap[TableRow]] = {
    val params =
      nio.TableRef.Parameters(schema, writeDisposition, createDisposition, tableDescription)
    self.asInstanceOf[SCollection[TableRow]].write(nio.TableRef(table))(params)
  }

  /**
   * Save this SCollection as a BigQuery table. Note that elements must be of type
   * [[com.google.api.services.bigquery.model.TableRow TableRow]].
   * @group output
   */
  def saveAsBigQuery(tableSpec: String, schema: TableSchema = null,
                     writeDisposition: WriteDisposition = null,
                     createDisposition: CreateDisposition = null,
                     tableDescription: String = null)
                    (implicit ev: T <:< TableRow): Future[Tap[TableRow]] = {
    val params =
      nio.TableSpec.Parameters(schema, writeDisposition, createDisposition, tableDescription)
    self.asInstanceOf[SCollection[TableRow]].write(nio.TableSpec(tableSpec))(params)
  }

  /**
   * Save this SCollection as a BigQuery table. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]].
   */
  def saveAsTypedBigQuery(table: TableReference,
                          writeDisposition: WriteDisposition,
                          createDisposition: CreateDisposition)
                         (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAnnotation)
  : Future[Tap[T]] = {
    val params = nio.Typed.Table.Parameters(writeDisposition, createDisposition)
    self.asInstanceOf[SCollection[T with HasAnnotation]]
      .write(nio.Typed.Table[T with HasAnnotation](table))(params)
      .asInstanceOf[Future[Tap[T]]]
  }

  /**
   * Save this SCollection as a BigQuery table. Note that element type `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType BigQueryType]].
   *
   * This could be a complete case class with
   * [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]]. For example:
   *
   * {{{
   * @BigQueryType.toTable
   * case class Result(name: String, score: Double)
   *
   * val p: SCollection[Result] = // process data and convert elements to Result
   * p.saveAsTypedBigQuery("myproject:mydataset.mytable")
   * }}}
   *
   * It could also be an empty class with schema from
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]], or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]. For
   * example:
   *
   * {{{
   * @BigQueryType.fromTable("publicdata:samples.gsod")
   * class Row
   *
   * sc.typedBigQuery[Row]()
   *   .sample(withReplacement = false, fraction = 0.1)
   *   .saveAsTypedBigQuery("myproject:samples.gsod")
   * }}}
   */
  def saveAsTypedBigQuery(tableSpec: String,
                          writeDisposition: WriteDisposition = null,
                          createDisposition: CreateDisposition = null)
                         (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAnnotation)
  : Future[Tap[T]] = {
    val params = nio.Typed.Table.Parameters(writeDisposition, createDisposition)
    self.asInstanceOf[SCollection[T with HasAnnotation]]
      .write(nio.Typed.Table[T with HasAnnotation](tableSpec))(params)
      .asInstanceOf[Future[Tap[T]]]
  }


  /**
   * Save this SCollection as a BigQuery TableRow JSON text file. Note that elements must be of
   * type [[com.google.api.services.bigquery.model.TableRow TableRow]].
   * @group output
   */
  def saveAsTableRowJsonFile(path: String,
                             numShards: Int = 0,
                             compression: Compression = Compression.UNCOMPRESSED)
                            (implicit ev: T <:< TableRow): Future[Tap[TableRow]] = {
    val params = nio.TableRowJsonFile.Parameters(numShards, compression)
    self.asInstanceOf[SCollection[TableRow]].write(nio.TableRowJsonFile(path))(params)
  }
}
