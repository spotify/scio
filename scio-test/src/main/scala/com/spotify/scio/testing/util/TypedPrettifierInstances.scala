package com.spotify.scio.testing.util

import com.spotify.scio.avro.TestRecord
import org.apache.avro.generic.IndexedRecord
import org.scalactic.Prettifier

import scala.collection.JavaConverters._

/**
 * Instances of [[TypedPrettifier]] which are found by implicit search.
 */
trait TypedPrettifierInstances extends LowPriorityFallbackTypedPrettifier {

  /**
   * An instance of TypedPrettifier when we have an AvroRecord.
   * We use the Avro Schema to create an table representation of
   * the SCollection[T]
   */
  implicit def forIndexedRecord[T <: IndexedRecord](
    implicit fallbackPrettifier: Prettifier
  ): TypedPrettifier[T] = new TypedPrettifier[T] {

    /** When applied on traversables */
    override def apply(t: Traversable[T]): String = {
      val fieldNames =
        t.headOption.map(_.getSchema.getFields.asScala).getOrElse(Nil).map(_.name())
      prettifyLevelOne(
        records = t,
        fieldNames = fieldNames,
        getFieldByIndex = (record: IndexedRecord, idx) => record.get(idx),
        levelTwoFallback = fallbackPrettifier
      )
    }

    /** When applied on a single Avro Record. */
    override def apply(t: T): String = fallbackPrettifier(t)
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
   * This `TypedPrettifier` falls back to the default scalactic [[Prettifier]].
   */
  implicit def default[T](
    implicit scalactic: Prettifier
  ): TypedPrettifier[T] =
    new TypedPrettifier[T] {
      override def apply(t: T): String = scalactic(t)
      override def apply(t: Traversable[T]): String = scalactic(t)
    }
}

object TypedPrettifierInstances extends TypedPrettifierInstances

object MyTest {

  import TypedPrettifierInstances._

  def main(args: Array[String]): Unit = {
    val avroRecods: Iterable[TestRecord] = (1 to 5).map(
      i =>
        TestRecord
          .newBuilder()
          .setIntField(i)
          .setStringField(i.toString)
          .setBooleanField(false)
          .setDoubleField(i / 2.0)
          .build())

    val pre = TypedPrettifier[com.spotify.scio.avro.TestRecord](avroRecods)
    def red(s: String) = Console.GREEN + s + Console.RESET

    println(s"pre = ${red(pre)}")
  }
}
