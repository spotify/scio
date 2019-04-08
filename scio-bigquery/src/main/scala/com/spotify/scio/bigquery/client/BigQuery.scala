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

package com.spotify.scio.bigquery.client

import java.io.{File, FileInputStream}

import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer
import com.spotify.scio.bigquery.client.BigQuery.Client
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{BigQuerySysProps, BigQueryType, CREATE_IF_NEEDED, WRITE_EMPTY}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

/** A simple BigQuery client. */
final class BigQuery private (client: Client) {
  private[scio] def isCacheEnabled: Boolean = BigQueryConfig.isCacheEnabled

  val jobs: JobOps = new JobOps(client)
  val tables: TableOps = new TableOps(client)
  val extract: ExtractOps = new ExtractOps(client, jobs)
  val load: LoadOps = new LoadOps(client, jobs)
  val query: QueryOps = new QueryOps(client, tables, jobs)

  // =======================================================================
  // Type safe API
  // =======================================================================

  /**
   * Get a typed iterator for a BigQuery SELECT query or table.
   *
   * Note that `T` must be annotated with [[BigQueryType.fromSchema]],
   * [[BigQueryType.fromTable]], [[BigQueryType.fromQuery]], or [[BigQueryType.toTable]].
   *
   * By default the source (table or query) specified in the annotation will be used, but it can
   * be overridden with the `newSource` parameter. For example:
   *
   * {{{
   * @BigQueryType.fromTable("publicdata:samples.gsod")
   * class Row
   *
   * // Read from [publicdata:samples.gsod] as specified in the annotation.
   * bq.getTypedRows[Row]()
   *
   * // Read from [myproject:samples.gsod] instead.
   * bq.getTypedRows[Row]("myproject:samples.gsod")
   *
   * // Read from a query instead.
   * bq.getTypedRows[Row]("SELECT * FROM [publicdata:samples.gsod] LIMIT 1000")
   * }}}
   */
  def getTypedRows[T <: HasAnnotation: TypeTag](newSource: String = null): Iterator[T] = {
    val bqt = BigQueryType[T]
    val rows = if (newSource == null) {
      // newSource is missing, T's companion object must have either table or query
      if (bqt.isTable) {
        tables.rows(bqt.table.get)
      } else if (bqt.isQuery) {
        query.rows(bqt.query.get)
      } else {
        throw new IllegalArgumentException("Missing table or query field in companion object")
      }
    } else {
      // newSource can be either table or query
      Try(BigQueryHelpers.parseTableSpec(newSource)).toOption
        .map(tables.rows)
        .getOrElse(query.rows(newSource))
    }
    rows.map(bqt.fromTableRow)
  }

  /**
   * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
   * [[BigQueryType]].
   */
  def writeTypedRows[T <: HasAnnotation: TypeTag](table: TableReference,
                                                  rows: List[T],
                                                  writeDisposition: WriteDisposition,
                                                  createDisposition: CreateDisposition): Long = {
    val bqt = BigQueryType[T]
    tables.writeRows(table,
                     rows.map(bqt.toTableRow),
                     bqt.schema,
                     writeDisposition,
                     createDisposition)
  }

  /**
   * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
   * [[BigQueryType]].
   */
  def writeTypedRows[T <: HasAnnotation: TypeTag](tableSpec: String,
                                                  rows: List[T],
                                                  writeDisposition: WriteDisposition = WRITE_EMPTY,
                                                  createDisposition: CreateDisposition =
                                                    CREATE_IF_NEEDED): Long =
    writeTypedRows(beam.BigQueryHelpers.parseTableSpec(tableSpec),
                   rows,
                   writeDisposition,
                   createDisposition)

  def createTypedTable[T <: HasAnnotation: TypeTag](table: Table): Unit =
    tables.create(table.setSchema(BigQueryType[T].schema))

  def createTypedTable[T <: HasAnnotation: TypeTag](table: TableReference): Unit =
    tables.create(table, BigQueryType[T].schema)

  def createTypedTable[T <: HasAnnotation: TypeTag](tableSpec: String): Unit =
    createTypedTable(beam.BigQueryHelpers.parseTableSpec(tableSpec))

  // =======================================================================
  // Job handling
  // =======================================================================

  /** Wait for all jobs to finish. */
  def waitForJobs(bqJobs: BigQueryJob*): Unit = jobs.waitForJobs(bqJobs: _*)
}

/** Companion object for [[BigQuery]]. */
object BigQuery {

  private lazy val instance: BigQuery =
    BigQuerySysProps.Project.valueOption.map(BigQuery(_)).getOrElse {
      Option(new DefaultProjectFactory().create(null))
        .map(BigQuery(_))
        .getOrElse {
          val flag = BigQuerySysProps.Project.flag
          throw new RuntimeException(s"Property $flag not set. Use -D$flag=<BILLING_PROJECT>")
        }
    }

  /**
   * Get the default BigQueryClient instance.
   *
   * Project must be set via `bigquery.project` system property.
   * An optional JSON secret file can be set via `bigquery.secret`.
   * For example, by adding the following code at the beginning of a job:
   * {{{
   * sys.props("bigquery.project") = "my-project"
   * sys.props("bigquery.secret") = "/path/to/secret.json"
   * }}}
   *
   * Or by passing them as SBT command line arguments:
   * {{{
   * sbt -Dbigquery.project=my-project -Dbigquery.secret=/path/to/secret.json
   * }}}
   */
  def defaultInstance(): BigQuery = instance

  /** Create a new BigQueryClient instance with the given project. */
  def apply(project: String): BigQuery =
    BigQuerySysProps.Secret.valueOption
      .map { secret =>
        BigQuery(project, new File(secret))
      }
      .getOrElse {
        BigQuery(project,
                 GoogleCredentials.getApplicationDefault.createScoped(BigQueryConfig.scopes.asJava))
      }

  /** Create a new BigQueryClient instance with the given project and secret file. */
  def apply(project: String, secretFile: File): BigQuery =
    BigQuery(project,
             GoogleCredentials
               .fromStream(new FileInputStream(secretFile))
               .createScoped(BigQueryConfig.scopes.asJava))

  /** Create a new BigQueryClient instance with the given project and credential. */
  def apply(project: String, credentials: => Credentials): BigQuery =
    new BigQuery(new Client(project, credentials))

  private[client] final class Client(val project: String, _credentials: => Credentials) {
    require(project != null && project.nonEmpty,
            "Invalid projectId. It should be a non-empty string")

    def credentials: Credentials = _credentials

    lazy val underlying: Bigquery = {
      val requestInitializer = new ChainingHttpRequestInitializer(
        new HttpCredentialsAdapter(credentials),
        new HttpRequestInitializer {
          override def initialize(request: HttpRequest): Unit = {
            BigQueryConfig.connectTimeoutMs.foreach(request.setConnectTimeout)
            BigQueryConfig.readTimeoutMs.foreach(request.setReadTimeout)
          }
        }
      )
      new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, requestInitializer)
        .setApplicationName("scio")
        .build()
    }
  }
}
