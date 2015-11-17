package com.spotify.scio.io

import java.util.UUID

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.{BigQueryClient, TableRow}
import com.spotify.scio.coders.KryoAtomicCoder
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.reflect.ClassTag

/**
 * Placeholder to an external data set that can be either read into memory or opened as an SCollection.
 */
trait Tap[T] {

  /** Read data set into memory. */
  def value: Iterator[T]

  /** Open data set as an SCollection. */
  def open(sc: ScioContext): SCollection[T]
}

/** Tap for text files on local file system or GCS. */
case class TextTap(path: String) extends Tap[String] {
  override def value: Iterator[String] = FileStorage(path).textFile
  override def open(sc: ScioContext): SCollection[String] = sc.textFile(path)
}

/** Tap for generic Avro files on local file system or GCS. */
case class GenericAvroTap(path: String, schema: Schema) extends Tap[GenericRecord] {
  override def value: Iterator[GenericRecord] = FileStorage(path).genericAvroFile
  override def open(sc: ScioContext): SCollection[GenericRecord] = sc.avroFile(path, schema)
}

/** Tap for specific Avro files on local file system or GCS. */
case class SpecificAvroTap[T: ClassTag](path: String) extends Tap[T] {
  override def value: Iterator[T] = FileStorage(path).specificAvroFile
  override def open(sc: ScioContext): SCollection[T] = sc.avroFile[T](path)
}

/** Tap for JSON files on local file system or GCS. */
case class TableRowJsonTap(path: String) extends Tap[TableRow] {
  override def value: Iterator[TableRow] = FileStorage(path).tableRowJsonFile
  override def open(sc: ScioContext): SCollection[TableRow] = sc.tableRowJsonFile(path)
}

/** Tap for BigQuery tables. */
case class BigQueryTap(table: TableReference, opts: DataflowPipelineOptions) extends Tap[TableRow] {
  override def value: Iterator[TableRow] = BigQueryClient(opts.getProject, opts.getGcpCredential).getTableRows(table)
  override def open(sc: ScioContext): SCollection[TableRow] = sc.bigQueryTable(table)
}

private[scio] case class MaterializedTap[T: ClassTag](path: String) extends Tap[T] {
  private def decode(s: String) = CoderUtils.decodeFromBase64(KryoAtomicCoder[T], s)
  override def value: Iterator[T] = {
    val storage = FileStorage(path)
    val i = storage.textFile.map(decode).toList.iterator
    storage.delete()
    i
  }
  override def open(sc: ScioContext): SCollection[T] = sc.textFile(path).map(decode)
}

private[scio] class InMemoryTap[T: ClassTag] extends Tap[T] {
  private[scio] val id: String = UUID.randomUUID().toString
  override def value: Iterator[T] = InMemorySinkManager.get(id).iterator
  override def open(sc: ScioContext): SCollection[T] = sc.parallelize[T](InMemorySinkManager.get(id))
}
