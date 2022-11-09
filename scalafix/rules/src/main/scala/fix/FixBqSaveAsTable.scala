package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixBqSaveAsTable extends SemanticRule("FixBqSaveAsTable") {
  private val scoll = "com/spotify/scio/values/SCollection#"
  private val methodName = "com.spotify.scio.extra.bigquery.syntax.AvroToBigQuerySCollectionOps.saveAvroAsBigQuery"

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case a @ Term.Apply(fun, head :: tail) =>
        if (fun.symbol.normalized.toString.contains(methodName)) {
          fun match {
            case Term.Select(qual, name) =>
              name match {
                case Term.Name("saveAvroAsBigQuery") if expectedType(qual, scoll) =>
                  // the rest of args should be named because the parameter order has changed
                  if (
                    tail.exists(!_.toString.contains("=")) ||
                      // `avroSchema` doesn't exist in the list of the new method
                      tail.exists(_.toString.contains("avroSchema"))
                  ) {
                    Patch.empty // not possible to fix, leave it to `LintBqSaveAsTable`
                  } else {
                    val headParam = if (head.toString.contains("=")) {
                      s"table = Table.Ref(${head.toString.split("=").last.trim})"
                    } else {
                      s"Table.Ref($head)"
                    }
                  val allArgs = (headParam :: tail).mkString(", ")
                  Patch.replaceTree(a, s"$qual.saveAsBigQueryTable($allArgs)")
                }
              case _ =>
                Patch.empty
            }
            case _ =>
              Patch.empty
          }
        } else {
          Patch.empty
        }
      case Importer(q"com.spotify.scio.extra.bigquery", imps) =>
          Patch.removeImportee(imps.head) +
            Patch.addGlobalImport(importer"com.spotify.scio.bigquery._")
      case _ => Patch.empty
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
