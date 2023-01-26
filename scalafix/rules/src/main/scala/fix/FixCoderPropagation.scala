package fix
package v0_10_0

import scalafix.v1._
import scala.meta._

object FixCoderPropagation {
  val Scoll: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/values/SCollection")
  val PairedScol: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/values/PairSCollectionFunctions")

  val CurriedSCollFns = Set(
    "top",
    "quantilesApprox"
  )

  val CurriedPairedScolFns = Set(
    "topByKey",
    "approxQuantilesByKey"
  )
}

class FixCoderPropagation extends SemanticRule("FixCoderPropagation") {

  import FixCoderPropagation._

  private def expectedType(
    expected: SymbolMatcher
  )(qual: Term)(implicit doc: SemanticDocument): Boolean =
    qual.symbol.info.get.signature match {
      case MethodSignature(_, _, TypeRef(_, typ, _)) =>
        expected.matches(typ)
      case ValueSignature(AnnotatedType(_, TypeRef(_, typ, _))) =>
        expected.matches(typ)
      case ValueSignature(TypeRef(_, typ, _)) =>
        Scoll.matches(typ)
      case _ =>
        false
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.$fn($arg1, $arg2)"
          if expectedType(Scoll)(qual) && CurriedSCollFns.contains(fn.value) =>
        Patch.replaceTree(t, q"$qual.$fn($arg1)($arg2)".syntax)
      case t @ q"$qual.$fn($arg1, $arg2)"
          if expectedType(PairedScol)(qual) && CurriedPairedScolFns.contains(fn.value) =>
        Patch.replaceTree(t, q"$qual.$fn($arg1)($arg2)".syntax)
    }.asPatch
  }
}
