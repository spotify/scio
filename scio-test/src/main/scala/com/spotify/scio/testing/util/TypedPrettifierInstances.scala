package com.spotify.scio.testing.util

import com.spotify.scio.schemas.Schema
import org.apache.avro.generic.IndexedRecord
import org.scalactic.Prettifier

import scala.collection.JavaConverters._

/**
 * Scalactic [[Prettifier]]s for `SCollection[T]`when we have a `Schema[T]` in scope.
 *
 * These prettifiers are used by [[TypedPrettifier]]
 */
trait TypedPrettifierInstances extends LowPriorityFallbackTypedPrettifier {

  /**
   * An instance of TypedPrettifier when we have an AvroRecord.
   * We use the Avro Schema to create an table representation of
   * the SCollection[T]
   */
  implicit def forIndexedRecord[T <: IndexedRecord](
    fallbackPrettifier: Prettifier
  ): TypedPrettifier[T] = new TypedPrettifier[T] {

    /**
     * The scalatic prettifier for [[IndexedRecord]].
     */
    override def apply: Prettifier = {
      case i: Traversable[_] => // TODO get type of the inner type.
        val indexed = i.map(_.asInstanceOf[IndexedRecord])
        val fieldNames =
          indexed.headOption.map(_.getSchema.getFields.asScala).getOrElse(Nil).map(_.name())
        prettifyLevelOne(
          records = indexed,
          fieldNames = fieldNames,
          getFieldByIndex = (record: IndexedRecord, idx) => record.get(idx),
          levelTwoFallback = fallbackPrettifier
        )
      case o =>
        fallbackPrettifier.apply(o)
    }
  }

  /**
   * Prettify one level of the traversable into a tabular representation
   * Do not create tables out of nested fields.
   */
  private def prettifyLevelOne[T](
    records: Traversable[T],
    fieldNames: Iterable[String],
    getFieldByIndex: (T, Int) => Any,
    levelTwoFallback: Prettifier
  ): String = {
    if (records.isEmpty) {
      "<No Record / Empty>"
    } else {
      def toPrettyRecord(colsAndPaddingLength: Iterable[(String, Int)]): String =
        colsAndPaddingLength
          .map {
            case (value, pad) =>
              val spaces = List.fill(pad - value.length)(' ').mkString("")
              val (pre, post) = spaces.splitAt(spaces.length / 2)
              s"$pre$value$post"
          }
          .mkString("│", "│", "│")

      def headerLine(padLengths: Iterable[Int]): String =
        padLengths.map(l => "─" * l).mkString("┌", "┬", "┐")

      def lineSeparator(padLengths: Iterable[Int]): String =
        padLengths.map(l => "─" * l).mkString("├", "┼", "┤")

      def footerLine(padLengths: Iterable[Int]): String =
        padLengths.map(l => "─" * l).mkString("└", "┴", "┘")

      val padLengths = fieldNames.zipWithIndex.map {
        case (fieldName, idx) =>
          val cols = records.map(record => getFieldByIndex(record, idx)).toSeq ++ Seq(fieldName)
          val asPrettyStrs = cols.map(levelTwoFallback(_))
          asPrettyStrs.map(_.length).max
      }

      val prettyRecords = records.map { record =>
        toPrettyRecord(padLengths.zipWithIndex.map {
          case (padTo, idx) =>
            (levelTwoFallback.apply(getFieldByIndex(record, idx)), padTo)
        })
      }

      (Seq(headerLine(padLengths),
           toPrettyRecord(fieldNames.zip(padLengths)),
           lineSeparator(padLengths))
        ++ prettyRecords
        ++ Seq(
          footerLine(padLengths)
        )).mkString("\n", "\n", "\n")
    }
  }

}

/**
 * A low priority fall back [[TypedPrettifier]] which delegates Scalactic's Prettifier.
 */
trait LowPriorityFallbackTypedPrettifier {

  /**
   * Visible when we fail to have a [[Schema]]
   *
   * This `TypedPrettifier` fallsback to the default scalactic Prettifier.
   */
  implicit def default[T](
    implicit scalactic: Prettifier
  ): TypedPrettifier[T] =
    new TypedPrettifier[T] {
      override def apply: Prettifier = scalactic
    }
}

object TypedPrettifierInstances extends TypedPrettifierInstances
