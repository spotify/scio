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

import java.io.File

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
import org.apache.beam.sdk.io.gcp.{bigquery => bq}

import scala.util.Try

private[scio] object Cache {

  private[scio] def isCacheEnabled: Boolean = BigQueryConfig.isCacheEnabled

  private[bigquery] def withCacheKey(key: String)(method: => TableSchema): TableSchema =
    if (isCacheEnabled) {
      getCacheSchema(key) match {
        case Some(schema) => schema
        case None =>
          val schema = method
          setCacheSchema(key, schema)
          schema
      }
    } else {
      method
    }

  private def setCacheSchema(key: String, schema: TableSchema): Unit =
    Files.write(schema.toPrettyString, schemaCacheFile(key), Charsets.UTF_8)

  private def getCacheSchema(key: String): Option[TableSchema] =
    Try {
      BigQueryUtil.parseSchema(scala.io.Source.fromFile(schemaCacheFile(key)).mkString)
    }.toOption

  private[bigquery] def setCacheDestinationTable(key: String, table: TableReference): Unit =
    Files.write(bq.BigQueryHelpers.toTableSpec(table), tableCacheFile(key), Charsets.UTF_8)

  private[bigquery] def getCacheDestinationTable(key: String): Option[TableReference] =
    Try {
      bq.BigQueryHelpers.parseTableSpec(scala.io.Source.fromFile(tableCacheFile(key)).mkString)
    }.toOption

  private def cacheFile(key: String, suffix: String): File = {
    val cacheDir = BigQueryConfig.cacheDirectory
    val filename = Hashing.murmur3_128().hashString(key, Charsets.UTF_8).toString + suffix
    val cacheFile = new File(s"$cacheDir/$filename")
    Files.createParentDirs(cacheFile)
    cacheFile
  }

  private def schemaCacheFile(key: String): File = cacheFile(key, ".schema.json")

  private def tableCacheFile(key: String): File = cacheFile(key, ".table.txt")
}
