package fix.v0_14_0

import scalafix.v1._
import scala.meta._

object FixQuery {
  val HasQueryMatcher: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/bigquery/types/BigQueryType/HasQuery#query")
}

class FixQuery extends SemanticRule("FixQuery") {

  import FixQuery._

  private def isQuery(term: Tree)(implicit doc: SemanticDocument): Boolean = {
    term.symbol.info.exists { info =>
      info.overriddenSymbols.exists(HasQueryMatcher.matches)
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.$fn" if HasQueryMatcher.matches(fn) =>
        Patch.replaceTree(t, q"$qual.queryRaw".syntax)
      case t @ q"$qual.query" if isQuery(t) =>
        Patch.replaceTree(t, q"$qual.queryRaw".syntax)
    }.asPatch
  }

}
