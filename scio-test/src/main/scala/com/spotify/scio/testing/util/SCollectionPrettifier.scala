package com.spotify.scio.testing.util

import com.spotify.scio.schemas.{Fallback, Schema, SchemaMaterializer}
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.values.Row
import org.scalactic.Prettifier

import scala.collection.JavaConverters._

/**
 * Scalactic [[Prettifier]]s for `SCollection[T]`when we have a `Schema[T]` in scope.
 *
 * These prettifiers are used by [[TypedPrettifier]]
 */
object SCollectionPrettifier {
  def getPrettifier[T](schema: Schema[T], fallbackPrettifier: Prettifier): Prettifier =
    schema match {
      // We prettify only when the Schema derivation is successful
      case Fallback(_) => fallbackPrettifier
      case _ =>
        new Prettifier {
          override def apply(o: Any): String = {
            val (bSchema, toRow, _) = SchemaMaterializer.materializeWithDefault(schema) // TODO pass scio context
            o match {
              case i: Traversable[_] =>
                prettifyLevelOne(i.map(_.asInstanceOf[T]).map(toRow(_)),
                                 bSchema,
                                 fallbackPrettifier)
              case _ =>
                fallbackPrettifier.apply(o)
            }
          }
        }
    }

  private def prettifyLevelOne(
    records: Traversable[Row],
    schema: BSchema,
    levelTwoFallback: Prettifier
  ): String = {
    if (records.isEmpty) {
      "<No Record / Empty>"
    } else {
      def toPrettyRecord(cols: Iterable[String]): String =
        cols.map(value => f"${value.toString}%-30s").mkString("│", "│", "│")

      def headerLine(cols: Int): String =
        (1 to cols).map(_ => "─" * 30).mkString("┌", "┬", "┐")

      def lineSeparator(cols: Int): String =
        (1 to cols).map(_ => "─" * 30).mkString("├", "┼", "┤")

      def footerLine(cols: Int): String =
        (1 to cols).map(_ => "─" * 30).mkString("└", "┴", "┘")

      val schemaFields = records.headOption.map(_.getSchema.getFields.asScala).getOrElse(Nil)
      val numFields = schemaFields.size

      val fieldNames = schema.getFieldNames.asScala

      val prettyRecords = records.map { record =>
        toPrettyRecord(fieldNames.map { f =>
          levelTwoFallback.apply(record.getValue[Any](f))
        })
      }

      (Seq(headerLine(numFields), toPrettyRecord(fieldNames), lineSeparator(numFields))
        ++ prettyRecords
        ++ Seq(
          footerLine(numFields)
        )).mkString("\n", "\n", "\n")
    }
  }
}
