package fix.v0_7_0

import scalafix.v1._

import scala.meta._

object BQClientRefactoring {
  val importClient = importer"com.spotify.scio.bigquery.client.BigQuery"
}

class BQClientRefactoring extends SyntacticRule("BQClientRefactoring") {

  import BQClientRefactoring._

  override def fix(implicit doc: SyntacticDocument): Patch = {
    val patch = doc.tree.collect {
      case i @ importee"BigQueryClient" =>
        Patch.removeImportee(i.asInstanceOf[Importee])
      case q"$expr.extractLocation(..$_)" =>
        Patch.addRight(expr, ".query")
      case q"$expr.extractTables(..$_)" =>
        Patch.addRight(expr, ".query")
      case t @ q"BigQueryClient.defaultInstance(..$params)" =>
        Patch.replaceTree(t, q"BigQuery.defaultInstance(..$params)".syntax)
      case t @ q"$expr.getQuerySchema(..$params)" =>
        Patch.replaceTree(t, q"$expr.query.schema(..$params)".syntax)
      case t @ q"$expr.getQueryRows(..$params)" =>
        Patch.replaceTree(t, q"$expr.query.rows(..$params)".syntax)
      case t @ q"$expr.getTableSchema(..$params)" =>
        Patch.replaceTree(t, q"$expr.tables.schema(..$params)".syntax)
      case t @ q"$expr.createTable(..$params)" =>
        Patch.replaceTree(t, q"$expr.tables.create(..$params)".syntax)
      case t @ q"$expr.getTable(..$params)" =>
        Patch.replaceTree(t, q"$expr.tables.table(..$params)".syntax)
      case t @ q"$expr.getTables(..$params)" =>
        Patch.replaceTree(t, q"$expr.tables.tableReferences(..$params)".syntax)
      case t @ q"$expr.getTableRows(..$params)" =>
        Patch.replaceTree(t, q"$expr.tables.rows(..$params)".syntax)
      case t @ q"$expr.loadTableFromCsv(..$params)" =>
        Patch.replaceTree(t, q"$expr.load.csv(..$params).get".syntax)
      case t @ q"$expr.loadTableFromJson(..$params)" =>
        Patch.replaceTree(t, q"$expr.load.json(..$params).get".syntax)
      case t @ q"$expr.loadTableFromAvro(..$params)" =>
        Patch.replaceTree(t, q"$expr.load.avro(..$params).get".syntax)
      case t @ q"$expr.exportTableAsCsv(..$params)" =>
        Patch.replaceTree(t, q"$expr.extract.asCsv(..$params)".syntax)
      case t @ q"$expr.exportTableAsJson(..$params)" =>
        Patch.replaceTree(t, q"$expr.extract.asJson(..$params)".syntax)
      case t @ q"$expr.exportTableAsAvro(..$params)" =>
        Patch.replaceTree(t, q"$expr.extract.asAvro(..$params)".syntax)
    }.asPatch

    if (patch.nonEmpty) patch + Patch.addGlobalImport(importClient) else Patch.empty
  }
}
