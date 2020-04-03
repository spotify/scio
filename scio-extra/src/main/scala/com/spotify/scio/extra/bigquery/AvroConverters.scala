package com.spotify.scio.extra.bigquery

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.annotations.experimental
import com.spotify.scio.bigquery.TableRow
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import scala.collection.JavaConverters._

object AvroConverters extends ToTableRow with ToTableSchema {

  @experimental
  def toTableRow[T](record: T)(implicit ev: T <:< IndexedRecord): TableRow = {
    val row = new TableRow

    record.getSchema.getFields.asScala.foreach { field =>
      Option(record.get(field.pos)).foreach { fieldValue =>
        row.set(field.name, toTableRowField(fieldValue, field))
      }
    }

    row
  }

  /**
   * Traverses all fields of the supplied avroSchema and converts it into
   * a TableSchema containing TableFieldSchemas.
   *
   * @param avroSchema
   * @return the equivalent BigQuery schema
   */
  @experimental
  def toTableSchema(avroSchema: Schema): TableSchema = {
    val fields = getFieldSchemas(avroSchema)

    new TableSchema().setFields(fields.asJava)
  }

  final case class AvroConversionException(
    private val message: String,
    private val cause: Throwable = null
  ) extends Exception(message, cause)
}
