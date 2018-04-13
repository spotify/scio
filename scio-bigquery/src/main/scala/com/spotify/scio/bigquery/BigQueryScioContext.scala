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

import java.beans.Introspector
import java.io.File
import java.net.URI
import java.nio.file.Files

import com.google.api.services.bigquery.model.TableReference
import com.google.datastore.v1.{Entity, Query}
import com.google.protobuf.Message
import com.spotify.scio.avro.types.AvroType
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{AvroBytesUtil, KryoAtomicCoder, KryoOptions}
import com.spotify.scio.io.Tap
import com.spotify.scio.metrics.Metrics
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing._
import com.spotify.scio.util._
import com.spotify.scio.values._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.extensions.gcp.options.{GcpOptions, GcsOptions}
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord
import org.apache.beam.sdk.io.gcp.{bigquery => bqio, datastore => dsio, pubsub => psio}
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{Create, DoFn, PTransform, SerializableFunction}
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{Pipeline, PipelineResult, io => gio}
import org.joda.time.Instant
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{Buffer => MBuffer}
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Enhanced version of [[ScioContext]] with BigQuery methods. */
final class BigQueryScioContext(@transient val self: ScioContext) extends Serializable {

    import self.{wrap, requireNotClosed, optionsAs}

    // =======================================================================
    // Miscellaneous
    // =======================================================================

    private lazy val bigQueryClient: BigQueryClient =
      self.cached[BigQueryClient]{
        val o = optionsAs[GcpOptions]
        BigQueryClient(o.getProject, o.getGcpCredential)
      }

    private def bqReadQuery[T: ClassTag](typedRead: bqio.BigQueryIO.TypedRead[T],
                                       sqlQuery: String,
                                       flattenResults: Boolean  = false)
  : SCollection[T] = requireNotClosed {
    if (self.isTest) {
      self.getTestInput(BigQueryIO[T](sqlQuery))
    } else if (bigQueryClient.isCacheEnabled) {
      val queryJob = bigQueryClient.newQueryJob(sqlQuery, flattenResults)

      self.onClose{ _ =>
        bigQueryClient.waitForJobs(queryJob)
      }

      val read = typedRead.from(queryJob.table).withoutValidation()
      wrap(self.applyInternal(read)).setName(sqlQuery)
    } else {
      val baseQuery = if (!flattenResults) {
        typedRead.fromQuery(sqlQuery).withoutResultFlattening()
      } else {
        typedRead.fromQuery(sqlQuery)
      }
      val query = if (bigQueryClient.isLegacySql(sqlQuery, flattenResults)) {
        baseQuery
      } else {
        baseQuery.usingStandardSql()
      }
      wrap(self.applyInternal(query)).setName(sqlQuery)
    }
  }

  private def avroBigQueryRead[T <: HasAnnotation : ClassTag : TypeTag] = {
    val fn = BigQueryType[T].fromAvro
    bqio.BigQueryIO
      .read(new SerializableFunction[SchemaAndRecord, T] {
        override def apply(input: SchemaAndRecord): T = fn(input.getRecord)
      })
      .withCoder(new KryoAtomicCoder[T](KryoOptions(self.options)))
  }

  private def bqReadTable[T: ClassTag](typedRead: bqio.BigQueryIO.TypedRead[T],
                                       table: TableReference)
  : SCollection[T] = requireNotClosed {
    val tableSpec: String = bqio.BigQueryHelpers.toTableSpec(table)
    if (self.isTest) {
      self.getTestInput(BigQueryIO[T](tableSpec))
    } else {
      wrap(self.applyInternal(typedRead.from(table))).setName(tableSpec)
    }
  }

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   * @group input
   */
  def bigQuerySelect(sqlQuery: String,
                     flattenResults: Boolean = false): SCollection[TableRow] =
    bqReadQuery(bqio.BigQueryIO.readTableRows(), sqlQuery, flattenResults)

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(table: TableReference): SCollection[TableRow] =
    bqReadTable(bqio.BigQueryIO.readTableRows(), table)

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    bigQueryTable(bqio.BigQueryHelpers.parseTableSpec(tableSpec))

  /**
   * Get a typed SCollection for a BigQuery SELECT query or table.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]], or
   * [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]].
   *
   * By default the source (table or query) specified in the annotation will be used, but it can
   * be overridden with the `newSource` parameter. For example:
   *
   * {{{
   * @BigQueryType.fromTable("publicdata:samples.gsod")
   * class Row
   *
   * // Read from [publicdata:samples.gsod] as specified in the annotation.
   * sc.typedBigQuery[Row]()
   *
   * // Read from [myproject:samples.gsod] instead.
   * sc.typedBigQuery[Row]("myproject:samples.gsod")
   *
   * // Read from a query instead.
   * sc.typedBigQuery[Row]("SELECT * FROM [publicdata:samples.gsod] LIMIT 1000")
   * }}}
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def typedBigQuery[T <: HasAnnotation : ClassTag : TypeTag](newSource: String = null)
  : SCollection[T] = {
    val bqt = BigQueryType[T]
    val typedRead = avroBigQueryRead[T]
    if (newSource == null) {
      // newSource is missing, T's companion object must have either table or query
      if (bqt.isTable) {
        bqReadTable(typedRead, bqio.BigQueryHelpers.parseTableSpec(bqt.table.get))
      } else if (bqt.isQuery) {
        bqReadQuery(typedRead, bqt.query.get)
      } else {
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
      }
    } else {
      // newSource can be either table or query
      val table = scala.util.Try(bqio.BigQueryHelpers.parseTableSpec(newSource)).toOption
      if (table.isDefined) {
        bqReadTable(typedRead, table.get)
      } else {
        bqReadQuery(typedRead, newSource)
      }
    }
  }

  /**
   * Get an SCollection for a BigQuery TableRow JSON file.
   * @group input
   */
  def tableRowJsonFile(path: String): SCollection[TableRow] = requireNotClosed {
    if (self.isTest) {
      self.getTestInput[TableRow](TableRowJsonIO(path))
    } else {
      wrap(self.applyInternal(gio.TextIO.read().from(path))).setName(path)
        .map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))
    }
  }

}
