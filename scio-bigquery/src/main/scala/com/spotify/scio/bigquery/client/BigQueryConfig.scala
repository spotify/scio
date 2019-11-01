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

package com.spotify.scio.bigquery.client

import java.nio.file.{Path, Paths}

import com.google.api.services.bigquery.BigqueryScopes
import com.spotify.scio.CoreSysProps
import com.spotify.scio.bigquery.BigQuerySysProps
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority

import scala.util.Try

object BigQueryConfig {
  /** Default cache directory. */
  private[this] val CacheDirectoryDefault: Path = Paths
    .get(CoreSysProps.TmpDir.value)
    .resolve(s"scio-bigquery-${CoreSysProps.User.value}")
    .resolve(".bigquery")

  /** Default cache behavior is enabled. */
  private[this] val CacheEnabledDefault: Boolean = true

  /** Default priority is batch. */
  private[this] val PriorityDefault: QueryPriority = QueryPriority.BATCH

  private[this] val DefaultScopes = List(BigqueryScopes.BIGQUERY)

  private[this] val DefaultLocation = "US"

  def location: String = DefaultLocation

  def scopes: Seq[String] = DefaultScopes

  def isCacheEnabled: Boolean =
    BigQuerySysProps.CacheEnabled.valueOption
      .flatMap(x => Try(x.toBoolean).toOption)
      .getOrElse(CacheEnabledDefault)

  def cacheDirectory: Path =
    BigQuerySysProps.CacheDirectory.valueOption.map(Paths.get(_)).getOrElse(CacheDirectoryDefault)

  def connectTimeoutMs: Option[Int] =
    BigQuerySysProps.ConnectTimeoutMs.valueOption.map(_.toInt)

  def readTimeoutMs: Option[Int] =
    BigQuerySysProps.ReadTimeoutMs.valueOption.map(_.toInt)

  def priority: QueryPriority = {
    lazy val isCompilingOrTesting = Thread
      .currentThread()
      .getStackTrace
      .exists { e =>
        e.getClassName.startsWith("scala.tools.nsc.interpreter.") ||
        e.getClassName.startsWith("org.scalatest.tools.")
      }

    BigQuerySysProps.Priority.valueOption.map(_.toUpperCase) match {
      case Some("INTERACTIVE")       => QueryPriority.INTERACTIVE
      case Some("BATCH")             => QueryPriority.BATCH
      case _ if isCompilingOrTesting => QueryPriority.INTERACTIVE
      case _                         => PriorityDefault
    }
  }
}
