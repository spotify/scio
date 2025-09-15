package fix.v0_14_0

import scalafix.v1._
import scala.meta._

object FixQuery {
  val HasQueryMatcher: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/bigquery/types/BigQueryType/HasQuery")
}

class FixQuery extends SemanticRule("FixQuery") {
  import FixQuery._

  private def isQueryFormat(term: Tree)(implicit doc: SemanticDocument): Boolean =
    term.symbol.info.map(_.signature).exists {
      case ClassSignature(_, parents, _, _) =>
        parents.exists {
          case TypeRef(_, symbol, _) if HasQueryMatcher.matches(symbol) => true
          case _                                                        => false
        }
      case _ => false
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.query" if isQueryFormat(qual) =>
        Patch.replaceTree(t, q"$qual.queryRaw".syntax)
    }.asPatch
  }

}
