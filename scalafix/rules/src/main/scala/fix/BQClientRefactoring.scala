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
    if(!imports.contains(t)) {
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

  override def fix(implicit doc: SyntacticDocument): Patch = {
    doc.tree.collect {
      case i @ Importee.Name(Name.Indeterminate("BigQueryClient")) =>
        Patch.removeImportee(i) + addImport(i.pos, importer"com.spotify.scio.bigquery.client.BigQuery")
      case BQDef(t, "extractLocation" | "extractTables") =>
        Patch.addLeft(t, "query.")
      case Term.Apply(Term.Select(n @ Term.Name("BigQueryClient"), Term.Name("defaultInstance")), _) =>
        Patch.replaceTree(n, "BigQuery")
      case BQDef(t, "getQuerySchema") =>
        Patch.replaceTree(t, "query.schema")
      case BQDef(t, "getQueryRows") =>
        Patch.replaceTree(t, "query.rows")
      case BQDef(t, "getTableSchema") =>
        Patch.replaceTree(t, "tables.schema")
      case BQDef(t, "getTableRows") =>
        Patch.replaceTree(t, "tables.rows")
      case BQDef(t, "loadTableFromCsv") =>
        Patch.replaceTree(t, "load.csv")
      case BQDef(t, "loadTableFromJson") =>
        Patch.replaceTree(t, "load.json")
      case BQDef(t, "loadTableFromAvro") =>
        Patch.replaceTree(t, "load.avro")
      case ap @ Term.Apply(Term.Select(bq, t @ Term.Name("getTable")), List(ref)) =>
        Patch.replaceTree(ap, s"${ref}.map(${bq}.tables.table)")
      case c if (c.toString.contains("BigQueryClient")) =>
        Patch.empty
    }.asPatch
  }
}
