package fix.v0_8_0

import scalafix.v1._

import scala.meta._

object FixSyntaxImports {
  object JDBC {
    val fns = List("JdbcScioContext", "JdbcSCollection")
    val `import` = importer"com.spotify.scio.jdbc._"
  }

  object BQ {
    val fns = List("toBigQueryScioContext", "toBigQuerySCollection")
    val `import` = importer"com.spotify.scio.bigquery._"
  }
}

final class FixSyntaxImports extends SemanticRule("FixSyntaxImports") {

  import FixSyntaxImports._

  override def fix(implicit doc: SemanticDocument): Patch = {
    val renameSymbols = Patch.replaceSymbols(
      "com/spotify/scio/transforms/AsyncLookupDoFn." -> "com/spotify/scio/transforms/BaseAsyncLookupDoFn.",
      "com.spotify.scio.jdbc.JdbcScioContext." -> "com.spotify.scio.jdbc.syntax.JdbcScioContextOps."
    )

    doc.tree.collect {
      case i @ importee"$name" if JDBC.fns.contains(name.value) =>
        Patch.removeImportee(i.asInstanceOf[Importee]) + Patch.addGlobalImport(JDBC.`import`)
      case i @ importee"$name" if BQ.fns.contains(name.value) =>
        Patch.removeImportee(i.asInstanceOf[Importee]) + Patch.addGlobalImport(BQ.`import`)
    }.asPatch + renameSymbols
  }
}
