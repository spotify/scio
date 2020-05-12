package com.spotify.scio.bigtable.syntax

import com.google.bigtable.v2.Row
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters._
import com.google.bigtable.v2.Cell

/** Enhanced version of `Row` with convenience methods. */
final class RowOps(private val self: Row) extends AnyVal {

  /** Return the `Cell`s for the specific column. */
  def getColumnCells(familyName: String, columnQualifier: ByteString): List[Cell] =
    (for {
      f <- self.getFamiliesList.asScala.find(_.getName == familyName)
      c <- f.getColumnsList.asScala.find(_.getQualifier == columnQualifier)
    } yield c.getCellsList.asScala).toList.flatten

  /** The `Cell` for the most recent timestamp for a given column. */
  def getColumnLatestCell(familyName: String, columnQualifier: ByteString): Option[Cell] =
    getColumnCells(familyName, columnQualifier).headOption

  /** Map of qualifiers to values. */
  def getFamilyMap(familyName: String): Map[ByteString, ByteString] =
    self.getFamiliesList.asScala.find(_.getName == familyName) match {
      case None => Map.empty
      case Some(f) =>
        if (f.getColumnsCount > 0) {
          f.getColumnsList.asScala
            .map(c => c.getQualifier -> c.getCells(0).getValue)
            .toMap
        } else {
          Map.empty
        }
    }

  /** Map of families to all versions of its qualifiers and values. */
  def getMap: Map[String, Map[ByteString, Map[Long, ByteString]]] = {
    val m = Map.newBuilder[String, Map[ByteString, Map[Long, ByteString]]]
    for (family <- self.getFamiliesList.asScala) {
      val columnMap = Map.newBuilder[ByteString, Map[Long, ByteString]]
      for (column <- family.getColumnsList.asScala) {
        val cellMap = column.getCellsList.asScala
          .map(x => x.getTimestampMicros -> x.getValue)
          .toMap
        columnMap += column.getQualifier -> cellMap
      }
      m += family.getName -> columnMap.result()
    }
    m.result()
  }

  /** Map of families to their most recent qualifiers and values. */
  def getNoVersionMap: Map[String, Map[ByteString, ByteString]] =
    self.getFamiliesList.asScala
      .map(f => f.getName -> getFamilyMap(f.getName))
      .toMap

  /** Get the latest version of the specified column. */
  def getValue(familyName: String, columnQualifier: ByteString): Option[ByteString] =
    for {
      f <- self.getFamiliesList.asScala.find(_.getName == familyName)
      c <- f.getColumnsList.asScala.find(_.getQualifier == columnQualifier)
    } yield c.getCells(0).getValue
}

trait RowSyntax {
  implicit def rowOps(row: Row): RowOps = new RowOps(row)
}
