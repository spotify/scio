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

import java.io.File

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.bigquery.BigQueryUtil
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files

import scala.util.Try
import org.apache.avro.Schema

private[client] object Cache {
  sealed trait Show[T] {
    def show(t: T): String
  }

  object Show {
    @inline final def apply[T](implicit t: Show[T]): Show[T] = t

    implicit val showTableSchema: Show[TableSchema] = new Show[TableSchema] {
      override def show(t: TableSchema): String = t.toPrettyString()
    }

    implicit val showTableRef: Show[TableReference] = new Show[TableReference] {
      override def show(table: TableReference): String =
        bq.BigQueryHelpers.toTableSpec(table)
    }

    implicit val showAvroSchema: Show[Schema] = new Show[Schema] {
      override def show(t: Schema): String = t.toString()
    }
  }

  sealed trait Read[T] {
    def read(s: String): Option[T]
  }

  object Read {
    @inline final def apply[T](implicit t: Read[T]): Read[T] = t

    implicit val readTableSchema: Read[TableSchema] = new Read[TableSchema] {
      override def read(s: String): Option[TableSchema] =
        Try(BigQueryUtil.parseSchema(s)).toOption
    }

    implicit val readTableRef: Read[TableReference] = new Read[TableReference] {
      override def read(table: String): Option[TableReference] =
        Try(bq.BigQueryHelpers.parseTableSpec(table)).toOption
    }

    implicit val readAvroSchema: Read[Schema] = new Read[Schema] {
      override def read(s: String): Option[Schema] =
        Try {
          new Schema.Parser().parse(s)
        }.toOption
    }
  }

  private[this] def isCacheEnabled: Boolean = BigQueryConfig.isCacheEnabled

  def getOrElse[T: Read: Show](key: String, f: String => File)(method: => T): T =
    if (isCacheEnabled) {
      get(key, f) match {
        case Some(schema) => schema
        case None         =>
          val schema = method
          set(key, schema, f)
          schema
      }
    } else {
      method
    }

  def set[T: Show](key: String, t: T, f: String => File): Unit =
    Files
      .asCharSink(f(key), Charsets.UTF_8)
      .write(Show[T].show(t))

  def get[T: Read](key: String, f: String => File): Option[T] =
    Try(scala.io.Source.fromFile(f(key)).mkString).toOption.flatMap(Read[T].read)

  val SchemaCache: String => File = key => cacheFile(key, ".schema.json")

  val TableCache: String => File = key => cacheFile(key, ".table.txt")

  private[this] def cacheFile(key: String, suffix: String): File = {
    val cacheDir = BigQueryConfig.cacheDirectory
    val filename = Hashing.murmur3_128().hashString(key, Charsets.UTF_8).toString + suffix
    val cacheFile = cacheDir.resolve(filename).toFile()
    Files.createParentDirs(cacheFile)
    cacheFile
  }
}
