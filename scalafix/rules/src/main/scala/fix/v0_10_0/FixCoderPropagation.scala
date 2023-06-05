package fix.v0_10_0

import scalafix.v1._

import scala.meta._

object FixCoderPropagation {
  val CurriedSCollFns = Seq("top", "quantilesApprox")
    .map(fn => SymbolMatcher.normalized(s"com/spotify/scio/values/SCollection#$fn"))

  val CurriedPairedSCollFns = Set("topByKey", "approxQuantilesByKey")
    .map(fn => SymbolMatcher.normalized(s"com/spotify/scio/values/PairSCollectionFunctions#$fn"))

  val CurriedFns = (CurriedSCollFns ++ CurriedPairedSCollFns).reduce(_ + _)
}

class FixCoderPropagation extends SemanticRule("FixCoderPropagation") {

  import FixCoderPropagation._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.$fn($arg1, $arg2)" if CurriedFns.matches(fn.symbol) =>
        Patch.replaceTree(t, q"$qual.$fn($arg1)($arg2)".syntax)
    }.asPatch
  }
}
