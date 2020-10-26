package fix
package v0_10_0

import scalafix.v1._
import scala.meta._

class FixCoderPropagation extends SemanticRule("FixCoderPropagation") {
  private val scoll = "com/spotify/scio/values/SCollection#"
  private val pairedScol = "com/spotify/scio/values/PairSCollectionFunctions#"

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect { case a @ Term.Apply(fun, List(arg1, arg2)) =>
      fun match {
        case Term.Select(qual, name) =>
          name match {
            case t @ Term.Name("top") if expectedType(qual, scoll) =>
              Patch.replaceTree(a, q"$fun($arg1)($arg2)".syntax)
            case t @ Term.Name("topByKey") if expectedType(qual, pairedScol) =>
              Patch.replaceTree(a, q"$fun($arg1)($arg2)".syntax)
            case t @ Term.Name("quantilesApprox") if expectedType(qual, scoll) =>
              Patch.replaceTree(a, q"$fun($arg1)($arg2)".syntax)
            case t @ Term.Name("approxQuantilesByKey") if expectedType(qual, pairedScol) =>
              Patch.replaceTree(a, q"$fun($arg1)($arg2)".syntax)
            case t =>
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
