package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class LintBqSaveAsTable extends SemanticRule("LintBqSaveAsTable") {
  private val scoll = "com/spotify/scio/values/SCollection#"

  case class BigQuerySaveBreakingSignature(fun: scala.meta.Term.Name) extends
    Diagnostic {
    override def position: Position = fun.pos
    override def message: String =
      s"`com.spotify.scio.bigquery.syntax.SCollectionBeamSchemaOps#saveAsBigQueryTable` should " +
        s"be used instead of $fun. Pay attention to the order of the method parameters because " +
        s"`avroSchema` is dropped from parameter list in the new method."
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect { case _@Term.Apply(fun, _ :: tail) =>
      fun match {
        case Term.Select(qual, name) =>
          name match {
            case t@Term.Name("saveAvroAsBigQuery") if expectedType(qual, scoll) =>
              // order of the parameters have changed in tail, so only named parameters can be
              // reused in the new method
              if (tail.exists(!_.toString.contains("=")) ||
                // avroSchema param is dropped in the new method
                tail.exists(_.toString.contains("avroSchema"))) {
                Patch.lint(BigQuerySaveBreakingSignature(t))
              } else {
                Patch.empty // `FixBqSaveAsTable` captures this
              }
            case _ =>
              Patch.empty
          }
      }
    }.asPatch
  }

  private def expectedType(qual: Term, typStr: String)(implicit doc: SemanticDocument): Boolean =
    qual.symbol.info.get.signature match {
      case MethodSignature(_, _, TypeRef(_, typ, _)) =>
        SymbolMatcher.exact(typStr).matches(typ)
      case ValueSignature(AnnotatedType(_, TypeRef(_, typ, _))) =>
        SymbolMatcher.exact(typStr).matches(typ)
      case ValueSignature(TypeRef(_, typ, _)) =>
        SymbolMatcher.exact(scoll).matches(typ)
      case t =>
        false
    }
}
