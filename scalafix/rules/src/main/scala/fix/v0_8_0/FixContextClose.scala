package fix.v0_8_0

import scalafix.v1._

import scala.meta._

object FixContextClose {
  val ScioContextClose = SymbolMatcher.normalized("com/spotify/scio/ScioContext#close")
}

final class FixContextClose extends SemanticRule("FixContextClose") {
  import FixContextClose._

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ q"$qual.$fn()" if ScioContextClose.matches(fn.symbol) =>
        Patch.replaceTree(t, q"$qual.run()".syntax)
    }.asPatch
}
