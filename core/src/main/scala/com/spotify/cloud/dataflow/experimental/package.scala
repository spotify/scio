package com.spotify.cloud.dataflow

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.{WriteDisposition, CreateDisposition}
import com.spotify.cloud.bigquery.Util
import com.spotify.cloud.dataflow.values.SCollection

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Main package for experimental APIs. Import all.
 *
 * {{{
 * import com.spotify.cloud.dataflow.experimental._
 * }}}
 */
package object experimental {

  /** Typed BigQuery annotations and converters. */
  val BigQueryType = com.spotify.cloud.bigquery.types.BigQueryType

  /** Enhanced version of [[DataflowContext]] with experimental features. */
  implicit class ExperimentalDataflowContext(val self: DataflowContext) extends AnyVal {

    /**
     * Get a typed SCollection for a BigQuery SELECT query or table.
     *
     * Note that `T` must be annotated with
     * [[com.spotify.cloud.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
     * [[com.spotify.cloud.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]], or
     * [[com.spotify.cloud.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]. For
     * example:
     *
     * {{{
     * @BigQueryType.fromTable("publicdata:samples.gsod")
     * class Row
     *
     * context.typedBigQuery[Row]()
     * }}}
     *
     * By default source (table or query) from the annotation will be used, but it can be
     * overridden with the `source` parameter.
     */
    def typedBigQuery[T: ClassTag : TypeTag](source: String = null): SCollection[T] = {
      val bqt = BigQueryType[T]

      if (bqt.isTable) {
        val src = if (source != null) source else Util.toTableSpec(bqt.table.get)
        self.bigQueryTable(src).map(bqt.fromTableRow)
      } else if (bqt.isQuery) {
        val src = if (source != null) source else bqt.query.get
        self.bigQuerySelect(src).map(bqt.fromTableRow)
      } else {
        throw new IllegalArgumentException(s"Missing table or query field in companion")
      }
    }

  }

  /**
   * Enhanced version of [[com.spotify.cloud.dataflow.values.SCollection SCollection]] with
   * experimental features.
   */
  implicit class ExperimentalSCollection[T](val self: SCollection[T]) extends AnyVal {

    /**
     * Save this SCollection as a Bigquery table. Note that element type `T` must be a case class
     * annotated with [[com.spotify.cloud.bigquery.types.BigQueryType.toTable
     * BigQueryType.toTable]].
     */
    def saveAsTypedBigQuery(table: TableReference,
                            createDisposition: CreateDisposition,
                            writeDisposition: WriteDisposition)
                           (implicit ct: ClassTag[T], tt: TypeTag[T]): Unit = {
      val bqt = BigQueryType[T]
      self
        .map(bqt.toTableRow)
        .saveAsBigQuery(table, bqt.schema, createDisposition, writeDisposition)
    }

    /**
     * Save this SCollection as a Bigquery table. Note that element type `T` must be a case class
     * annotated with [[com.spotify.cloud.bigquery.types.BigQueryType.toTable
     * BigQueryType.toTable]].
     */
    def saveAsTypedBigQuery(tableSpec: String,
                            createDisposition: CreateDisposition = null,
                            writeDisposition: WriteDisposition = null)
                           (implicit ct: ClassTag[T], tt: TypeTag[T]): Unit = {
      val bqt = BigQueryType[T]
      self
        .map(bqt.toTableRow)
        .saveAsBigQuery(tableSpec, bqt.schema, createDisposition, writeDisposition)
    }

  }

}
