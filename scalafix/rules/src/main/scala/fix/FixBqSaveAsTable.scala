package fix
package v0_12_0

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

  private def renameNamedParams(args: List[Term]): List[Term] =
    args.map {
      case q"avroSchema = $schema" => q"schema = toTableSchema($schema)"
      case q"table = $table"       => q"table = Table.Ref($table)"
      case p                       => p
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val patch = doc.tree.collect {
      case t @ q"$qual.$fn(..$args)" if SaveAvroAsBigQuery.matches(fn.symbol) =>
        val updatedArgs = args.toList match {
          case (head: Term.Assign) :: tail =>
            renameNamedParams(head :: tail)
          case table :: Nil =>
            q"Table.Ref($table)" :: Nil
          case table :: (second: Term.Assign) :: tail =>
            q"Table.Ref($table)" :: renameNamedParams(second :: tail)
          case table :: schema :: tail =>
            q"Table.Ref($table)" :: q"toTableSchema($schema)" :: tail
          case Nil =>
            throw new Exception("Missing required table argument in saveAvroAsBigQuery")
        }
        Patch.replaceTree(t, q"$qual.saveAsBigQueryTable(..$updatedArgs)".syntax)
    }.asPatch

    if (patch.nonEmpty) {
      patch + Patch.addGlobalImport(PackageImport) + Patch.addGlobalImport(ConverterImport)
    } else {
      Patch.empty
    }
  }
}
