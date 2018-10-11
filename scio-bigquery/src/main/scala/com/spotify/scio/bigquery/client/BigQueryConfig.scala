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

package com.spotify.scio.bigquery.client

import com.google.api.services.bigquery.BigqueryScopes
import com.spotify.scio.bigquery.BigQuerySysProps

import scala.util.Try

object BigQueryConfig {

  /** Default cache directory. */
  private[this] val CacheDirectoryDefault: String = sys.props("user.dir") + "/.bigquery"

  /** Default cache behavior is enabled. */
  private[this] val CacheEnabledDefault: Boolean = true

  private[this] val DefaultScopes = List(BigqueryScopes.BIGQUERY)

  private[this] val DefaultLocation = "US"

  def location: String = DefaultLocation

  def scopes: Seq[String] = DefaultScopes

  def isCacheEnabled: Boolean =
    BigQuerySysProps.CacheEnabled.valueOption
      .flatMap(x => Try(x.toBoolean).toOption)
      .getOrElse(CacheEnabledDefault)

  def cacheDirectory: String =
    BigQuerySysProps.CacheDirectory.value(CacheDirectoryDefault)

  def connectTimeoutMs: Option[Int] =
    BigQuerySysProps.ConnectTimeoutMs.valueOption.map(_.toInt)

  def readTimeoutMs: Option[Int] =
    BigQuerySysProps.ReadTimeoutMs.valueOption.map(_.toInt)

  def priority: Option[String] = BigQuerySysProps.Priority.valueOption

}
