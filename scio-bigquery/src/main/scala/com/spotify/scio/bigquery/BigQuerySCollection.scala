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

package com.spotify.scio
package bigquery

import java.io.PrintStream
import java.lang.{Boolean => JBoolean, Double => JDouble, Iterable => JIterable}
import java.util.concurrent.ThreadLocalRandom

import com.google.datastore.v1.Entity
import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.avro.types.AvroType
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.io._
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.testing._
import com.spotify.scio.util._
import com.spotify.scio.util.random.{BernoulliSampler, PoissonSampler}
import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.twitter.algebird.{Aggregator, Monoid, Semigroup}
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.io.gcp.{bigquery => bqio, datastore => dsio, pubsub => psio}
import org.apache.beam.sdk.io.{Compression, FileBasedSink}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{io => gio}
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.concurrent._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.spotify.scio.values.SCollection
import types.BigQueryType.HasAnnotation
import scala.language.implicitConversions

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class BigQuerySCollection[T](@transient val self: SCollection[T]) extends Serializable {

  import self.{context, saveAsInMemoryTap}

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
    val tableSpec = bqio.BigQueryHelpers.toTableSpec(table)
    if (context.isTest) {
      context.testOut(BigQueryIO[TableRow](tableSpec))(self.asInstanceOf[SCollection[TableRow]])

      if (writeDisposition == WriteDisposition.WRITE_APPEND) {
        Future.failed(new NotImplementedError("BigQuery future with append not implemented"))
      } else {
        saveAsInMemoryTap.asInstanceOf[Future[Tap[TableRow]]]
      }
    } else {
      var transform = bqio.BigQueryIO.writeTableRows().to(table)
      if (schema != null) transform = transform.withSchema(schema)
      if (createDisposition != null) transform = transform.withCreateDisposition(createDisposition)
      if (writeDisposition != null) transform = transform.withWriteDisposition(writeDisposition)
      if (tableDescription != null) transform = transform.withTableDescription(tableDescription)
      self.asInstanceOf[SCollection[TableRow]].applyInternal(transform)

      if (writeDisposition == WriteDisposition.WRITE_APPEND) {
        Future.failed(new NotImplementedError("BigQuery future with append not implemented"))
      } else {
        context.makeFuture(io.BigQueryTap(table))
      }
    }
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
                    (implicit ev: T <:< TableRow): Future[Tap[TableRow]] =
    saveAsBigQuery(
      bqio.BigQueryHelpers.parseTableSpec(tableSpec),
      schema,
      writeDisposition,
      createDisposition,
      tableDescription)

  /**
   * Save this SCollection as a BigQuery table. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]].
   */
  def saveAsTypedBigQuery(table: TableReference,
                          writeDisposition: WriteDisposition,
                          createDisposition: CreateDisposition)
                         (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAnnotation)
  : Future[Tap[T]] = {
    val tableSpec = bqio.BigQueryHelpers.toTableSpec(table)
    if (context.isTest) {
      context.testOut(BigQueryIO[T](tableSpec))(self.asInstanceOf[SCollection[T]])

      if (writeDisposition == WriteDisposition.WRITE_APPEND) {
        Future.failed(new NotImplementedError("BigQuery future with append not implemented"))
      } else {
        saveAsInMemoryTap
      }
    } else {
      val bqt = BigQueryType[T]
      val initialTfName = self.tfName
      import scala.concurrent.ExecutionContext.Implicits.global
      val scoll =
        self
          .map(bqt.toTableRow)
          .withName(s"$initialTfName$$Write")
      scoll.saveAsBigQuery(
          table,
          bqt.schema,
          writeDisposition,
          createDisposition,
          bqt.tableDescription.orNull)
        .map(_.map(bqt.fromTableRow))
    }
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
  : Future[Tap[T]] =
    saveAsTypedBigQuery(
      bqio.BigQueryHelpers.parseTableSpec(tableSpec),
      writeDisposition, createDisposition)


  /**
   * Save this SCollection as a BigQuery TableRow JSON text file. Note that elements must be of
   * type [[com.google.api.services.bigquery.model.TableRow TableRow]].
   * @group output
   */
  def saveAsTableRowJsonFile(path: String,
                             numShards: Int = 0,
                             compression: Compression = Compression.UNCOMPRESSED)
                            (implicit ev: T <:< TableRow): Future[Tap[TableRow]] = {
    if (context.isTest) {
      context.testOut(TableRowJsonIO(path))(self.asInstanceOf[SCollection[TableRow]])
      saveAsInMemoryTap.asInstanceOf[Future[Tap[TableRow]]]
    } else {
      self.asInstanceOf[SCollection[TableRow]]
        .map(e => ScioUtil.jsonFactory.toString(e))
        .applyInternal(self.textOut(path, ".json", numShards, compression))
      context.makeFuture(io.TableRowJsonTap(ScioUtil.addPartSuffix(path)))
    }
  }
}
