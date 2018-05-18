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

package com.spotify.scio.bigquery

import com.google.api.services.bigquery.BigqueryScopes
import scala.collection.JavaConverters._

import scala.util.Try

object BigQueryConfig {

  /** System property key for billing project. */
  val PROJECT_KEY: String = "bigquery.project"

  /** System property key for JSON secret path. */
  val SECRET_KEY: String = "bigquery.secret"

  /** System property key for local schema cache directory. */
  val CACHE_DIRECTORY_KEY: String = "bigquery.cache.directory"

  /** Default cache directory. */
  val CACHE_DIRECTORY_DEFAULT: String = sys.props("user.dir") + "/.bigquery"

  /** System property key for enabling or disabling scio bigquery caching */
  val CACHE_ENABLED_KEY: String = "bigquery.cache.enabled"

  /** Default cache behavior is enabled. */
  val CACHE_ENABLED_DEFAULT: Boolean = true

  /** System property key for priority, "BATCH" or "INTERACTIVE". */
  val PRIORITY_KEY = "bigquery.priority"

  /**
   * System property key for timeout in milliseconds to establish a connection.
   * Default is 20000 (20 seconds). 0 for an infinite timeout.
   */
  val CONNECT_TIMEOUT_MS_KEY: String = "bigquery.connect_timeout"

  /**
   * System property key for timeout in milliseconds to read data from an established connection.
   * Default is 20000 (20 seconds). 0 for an infinite timeout.
   */
  val READ_TIMEOUT_MS_KEY: String = "bigquery.read_timeout"

  val SCOPES: java.util.List[String] = List(BigqueryScopes.BIGQUERY).asJava

  /* caching config */
  def isCacheEnabled: Boolean = Option(sys.props(CACHE_ENABLED_KEY))
    .flatMap(x => Try(x.toBoolean).toOption).getOrElse(CACHE_ENABLED_DEFAULT)

  //private[bigquery]
  def cacheDirectory: String =
    getPropOrElse(CACHE_DIRECTORY_KEY, CACHE_DIRECTORY_DEFAULT)

  /* bigquery config */
  def connectTimeoutMs: Option[Int] = Option(sys.props(CONNECT_TIMEOUT_MS_KEY)).map(_.toInt)

  def readTimeoutMs: Option[Int] = Option(sys.props(READ_TIMEOUT_MS_KEY)).map(_.toInt)

  /* query job config */
  def priority: Option[String] = Option(sys.props(PRIORITY_KEY))

  def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

}
