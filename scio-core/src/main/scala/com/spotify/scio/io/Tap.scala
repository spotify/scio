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

package com.spotify.scio.io

import java.util.UUID

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.{BigQueryClient, TableRow}
import com.spotify.scio.coders.{KryoAtomicCoder, AvroBytesUtil}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.reflect.ClassTag

/**
 * Placeholder to an external data set that can either be load into memory as an iterator or opened
 * in a new ScioContext as an SCollection.
 */
trait Tap[T] { self =>

  /** Read data set into memory. */
  def value: Iterator[T]

  /** Open data set as an SCollection. */
  def open(sc: ScioContext): SCollection[T]

  /** Map items from T to U. */
  def map[U: ClassTag](f: T => U): Tap[U] = new Tap[U] {
    /** Read data set into memory. */
    override def value: Iterator[U] = self.value.map(f)

    /** Open data set as an SCollection. */
    override def open(sc: ScioContext): SCollection[U] = self.open(sc).map(f)
  }

}

/** Tap for text files on local file system or GCS. */
case class TextTap(path: String) extends Tap[String] {
  override def value: Iterator[String] = FileStorage(path).textFile
  override def open(sc: ScioContext): SCollection[String] = sc.textFile(path)
}

/** Tap for Avro files on local file system or GCS. */
case class AvroTap[T: ClassTag](path: String, schema: Schema = null) extends Tap[T] {
  override def value: Iterator[T] = FileStorage(path).avroFile(schema)
  override def open(sc: ScioContext): SCollection[T] = sc.avroFile[T](path, schema)
}

/** Tap for JSON files on local file system or GCS. */
case class TableRowJsonTap(path: String) extends Tap[TableRow] {
  override def value: Iterator[TableRow] = FileStorage(path).tableRowJsonFile
  override def open(sc: ScioContext): SCollection[TableRow] = sc.tableRowJsonFile(path)
}

/** Tap for BigQuery tables. */
case class BigQueryTap(table: TableReference) extends Tap[TableRow] {
  override def value: Iterator[TableRow] = BigQueryClient.defaultInstance().getTableRows(table)
  override def open(sc: ScioContext): SCollection[TableRow] = sc.bigQueryTable(table)
}

/** Tap for object files on local file system or GCS. */
case class ObjectFileTap[T: ClassTag](path: String) extends Tap[T] {
  override def value: Iterator[T] = {
    val coder = KryoAtomicCoder[T]
    FileStorage(path).avroFile[GenericRecord](AvroBytesUtil.schema).map { r =>
      AvroBytesUtil.decode(coder, r)
    }
  }
  override def open(sc: ScioContext): SCollection[T] = sc.objectFile(path)
}

private[scio] class InMemoryTap[T: ClassTag] extends Tap[T] {
  private[scio] val id: String = UUID.randomUUID().toString
  override def value: Iterator[T] = InMemorySinkManager.get(id).iterator
  override def open(sc: ScioContext): SCollection[T] =
    sc.parallelize[T](InMemorySinkManager.get(id))
}
