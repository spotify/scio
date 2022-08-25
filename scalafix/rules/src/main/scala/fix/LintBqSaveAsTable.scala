package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class LintBqSaveAsTable extends SemanticRule("LintBqSaveAsTable") {

  case class BigQuerySaveBreakingSignature(fun: scala.meta.Term.Name, msg: String) extends
    Diagnostic {
    override def position: Position = fun.pos
    override def message: String =
      s"`com.spotify.scio.bigquery.syntax.SCollectionBeamSchemaOps#saveAsBigQueryTable` should " +
        s"be used instead of $fun. Pay attention to the order of the method parameters because " +
        s"`avroSchema` is dropped from parameter list in the new method."
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect { case a@Term.Apply(fun, head :: tail) =>
      fun match {
        case Term.Select(qual, name) =>
          name match {
            case t@Term.Name("saveAvroAsBigQuery") =>
              if (tail.exists(!_.toString.contains("="))) {
                Patch.lint(BigQuerySaveBreakingSignature(t, "bad args msg"))
              } else if (tail.exists(_.toString.contains("avroSchema"))) {
                Patch.lint(BigQuerySaveBreakingSignature(t, "bad args msg"))
              } else {
                Patch.empty // `FixBqSaveAsTable` captures this
              }
            case _ =>
              Patch.empty
          }
      }
    }.asPatch
  }
}
