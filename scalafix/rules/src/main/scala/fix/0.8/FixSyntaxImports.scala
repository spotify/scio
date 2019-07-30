package fix
package v0_8_0

import scalafix.v1._
import scala.meta._

class FixSyntaxImports extends SemanticRule("FixSyntaxImports") {
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

  object JDBC {
    val fns =
      List("JdbcScioContext", "JdbcSCollection")

    val `import` = importer"com.spotify.scio.jdbc._"
  }

  object BQ {
    val fns =
      List("toBigQueryScioContext", "toBigQuerySCollection")

    val `import` = importer"com.spotify.scio.bigquery._"
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val renameSymbols =
      Patch.replaceSymbols(
        "com/spotify/scio/transforms/AsyncLookupDoFn." ->
          "com/spotify/scio/transforms/BaseAsyncLookupDoFn.",
        "com.spotify.scio.jdbc.JdbcScioContext." ->
          "com.spotify.scio.jdbc.syntax.JdbcScioContextOps."
      )

    doc.tree.collect {
      case i @ Importee.Name(Name.Indeterminate(n)) if JDBC.fns.contains(n) =>
        Patch.removeImportee(i) + addImport(i.pos, JDBC.`import`)
      case i @ Importee.Name(Name.Indeterminate(n)) if BQ.fns.contains(n) =>
        Patch.removeImportee(i) + addImport(i.pos, BQ.`import`)
    }.asPatch + renameSymbols
  }
}
