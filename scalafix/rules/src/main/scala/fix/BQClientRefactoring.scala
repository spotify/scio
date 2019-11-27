package fix
package v0_7_0

import scalafix.v1._
import scala.meta._

class BQClientRefactoring extends SyntacticRule("BQClientRefactoring") {
  private val imports =
    scala.collection.mutable.ArrayBuffer.empty[(String, String)]

  // Check that the package is not imported multiple times in the same file
  def addImport(p: Position, i: Importer) = {
    val Importer(s) = i
    val Input.VirtualFile(path, _) = p.input

    val t = (s.toString, path)
    if (!imports.contains(t)) {
      imports += t
      Patch.addGlobalImport(i)
    } else Patch.empty
  }

  object BQDef {
    def unapply(t: Tree) =
      t match {
        case Term.Apply(Term.Select(_, t @ Term.Name(c)), _) =>
          Option((t, c))
        case _ => None
      }
  }

  def addBQImport(i: Tree) =
    addImport(i.pos, importer"com.spotify.scio.bigquery.client.BigQuery")

  override def fix(implicit doc: SyntacticDocument): Patch = {
    doc.tree.collect {
      case i @ Importee.Name(Name.Indeterminate("BigQueryClient")) =>
        Patch.removeImportee(i) + addBQImport(i)
      case BQDef(t, "extractLocation" | "extractTables") =>
        Patch.addLeft(t, "query.") + addBQImport(t)
      case Term.Apply(
          Term.Select(n @ Term.Name("BigQueryClient"), Term.Name("defaultInstance")),
          _
          ) =>
        Patch.replaceTree(n, "BigQuery") + addBQImport(n)
      case BQDef(t, "getQuerySchema") =>
        Patch.replaceTree(t, "query.schema") + addBQImport(t)
      case BQDef(t, "getQueryRows") =>
        Patch.replaceTree(t, "query.rows") + addBQImport(t)
      case BQDef(t, "getTableSchema") =>
        Patch.replaceTree(t, "tables.schema") + addBQImport(t)
      case BQDef(t, "createTable") =>
        Patch.replaceTree(t, "tables.create") + addBQImport(t)
      case BQDef(t, "getTable") =>
        Patch.replaceTree(t, "tables.table") + addBQImport(t)
      case BQDef(t, "getTables") =>
        Patch.replaceTree(t, "tables.tableReferences") + addBQImport(t)
      case BQDef(t, "getTableRows") =>
        Patch.replaceTree(t, "tables.rows") + addBQImport(t)
      case ap @ BQDef(t, "loadTableFromCsv") =>
        Patch.addRight(ap, ".get") + Patch.replaceTree(t, "load.csv") + addBQImport(t)
      case ap @ BQDef(t, "loadTableFromJson") =>
        Patch.addRight(ap, ".get") + Patch.replaceTree(t, "load.json") + addBQImport(t)
      case ap @ BQDef(t, "loadTableFromAvro") =>
        Patch.addRight(ap, ".get") + Patch.replaceTree(t, "load.avro") + addBQImport(t)
      case BQDef(t, "exportTableAsCsv") =>
        Patch.replaceTree(t, "extract.asCsv") + addBQImport(t)
      case BQDef(t, "exportTableAsJson") =>
        Patch.replaceTree(t, "extract.asJson") + addBQImport(t)
      case BQDef(t, "exportTableAsAvro") =>
        Patch.replaceTree(t, "extract.asAvro") + addBQImport(t)
      case c if c.toString.contains("BigQueryClient") =>
        Patch.empty
    }.asPatch
  }
}
