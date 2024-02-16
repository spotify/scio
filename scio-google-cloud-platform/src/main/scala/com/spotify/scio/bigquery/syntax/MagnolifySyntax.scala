package com.spotify.scio.bigquery.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.{Query, Source, Table}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.sun.tools.javac.code.TypeTag
import magnolify.bigquery.TableRowType

final class MagnolifyBigQueryScioContextOps(private val self: ScioContext) extends AnyVal {

  def typedBigQuery[T : TableRowType : TypeTag: Coder](): SCollection[T] =
    typedBigQuery(None)

  def typedBigQuery[T : TableRowType: TypeTag: Coder](
    newSource: Source
  ): SCollection[T] = typedBigQuery(Option(newSource))

  /** Get a typed SCollection for BigQuery Table or a SELECT query using the Storage API. */
  def typedBigQuery[T : TableRowType: TypeTag: Coder](
    newSource: Option[Source]
  ): SCollection[T] = {
    val bqt = BigQueryType[T]
    if (bqt.isStorage) {
      newSource
        .asInstanceOf[Option[Table]]
        .map(typedBigQueryStorage(_))
        .getOrElse(typedBigQueryStorage())
    } else {
      self.read(BigQueryTyped.dynamic[T](newSource))
    }
  }

  /**
   * Get a typed SCollection for a BigQuery storage API.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromStorage]] or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]
   */
  def typedBigQueryStorage[T : TableRowType: TypeTag: Coder](): SCollection[T] = {
    val bqt = BigQueryType[T]
    if (bqt.isQuery) {
      self.read(BigQueryTyped.StorageQuery[T](Query(bqt.queryRaw.get)))
    } else {
      val table = Table.Spec(bqt.table.get)
      val rr = bqt.rowRestriction
      val fields = bqt.selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields)
      self.read(BigQueryTyped.Storage[T](table, fields, rr))
    }
  }

  def typedBigQueryStorage[T : TableRowType: TypeTag: Coder](
    table: Table
  ): SCollection[T] =
    self.read(
      BigQueryTyped.Storage[T](
        table,
        BigQueryType[T].selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        BigQueryType[T].rowRestriction
      )
    )

  def typedBigQueryStorage[T : TableRowType: TypeTag: Coder](
    rowRestriction: String
  ): SCollection[T] = {
    val bqt = BigQueryType[T]
    val table = Table.Spec(bqt.table.get)
    self.read(
      BigQueryTyped.Storage[T](
        table,
        bqt.selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        Option(rowRestriction)
      )
    )
  }

  def typedBigQueryStorage[T : TableRowType: TypeTag: Coder](
    table: Table,
    rowRestriction: String
  ): SCollection[T] =
    self.read(
      BigQueryTyped.Storage[T](
        table,
        BigQueryType[T].selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        Option(rowRestriction)
      )
    )

  def typedBigQueryStorage[T : TableRowType: TypeTag: Coder](
    table: Table,
    selectedFields: List[String],
    rowRestriction: String
  ): SCollection[T] =
    self.read(
      BigQueryTyped.Storage[T](
        table,
        selectedFields,
        Option(rowRestriction)
      )
    )
}

trait MagnolifySyntax {
  implicit def magnolifyBigQueryScioContextOps(sc: ScioContext): MagnolifyBigQueryScioContextOps =
    new MagnolifyBigQueryScioContextOps(sc)
}
