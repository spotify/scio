package fix.v0_12_0

import scalafix.v1._

import scala.meta._

object FixBqSaveAsTable {
  val SaveAvroAsBigQuery = SymbolMatcher.normalized(
    "com/spotify/scio/extra/bigquery/syntax/AvroToBigQuerySCollectionOps#saveAvroAsBigQuery"
  )

  val PackageImport = importer"com.spotify.scio.bigquery._"
  val ConverterImport = importer"com.spotify.scio.extra.bigquery.AvroConverters.toTableSchema"
}

class FixBqSaveAsTable extends SemanticRule("FixBqSaveAsTable") {

  import FixBqSaveAsTable._

  private def updateParams(args: List[Term]): List[Term] =
    args.zipWithIndex.map {
      case (p, 0) if !p.isInstanceOf[Term.Assign] => q"Table.Ref($p)"
      case (p, 1) if !p.isInstanceOf[Term.Assign] => q"toTableSchema($p)"
      case (q"avroSchema = $schema", _)           => q"schema = toTableSchema($schema)"
      case (q"table = $table", _)                 => q"table = Table.Ref($table)"
      case (p, _)                                 => p
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.$fn(..$args)" if SaveAvroAsBigQuery.matches(fn.symbol) =>
        val updatedArgs = updateParams(args)
        Patch.replaceTree(t, q"$qual.saveAsBigQueryTable(..$updatedArgs)".syntax) +
          Patch.addGlobalImport(PackageImport) +
          Patch.addGlobalImport(ConverterImport)
    }.asPatch
  }
}
