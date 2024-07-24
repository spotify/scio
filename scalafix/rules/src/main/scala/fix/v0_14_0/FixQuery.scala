package fix.v0_14_0

import scalafix.v1._
import scala.meta._

object FixQuery {
  val HasQueryMatcher: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/bigquery/types/BigQueryType/HasQuery")

  val HasQueryMethodMatcher: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/bigquery/types/BigQueryType/HasQuery#query")
}

class FixQuery extends SemanticRule("FixQuery") {

  import FixQuery._

  private def isQuery(term: Tree)(implicit doc: SemanticDocument): Boolean =
    term.symbol.info.exists(_.overriddenSymbols.exists(HasQueryMethodMatcher.matches))

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
      case t @ q"$qual.$fn" if HasQueryMethodMatcher.matches(fn) =>
        Patch.replaceTree(t, q"$qual.queryRaw".syntax)
      case t @ q"$qual.query" if isQuery(t) =>
        Patch.replaceTree(t, q"$qual.queryRaw".syntax)
      case t @ q"$qual.query.format" if isQueryFormat(qual) =>
        Patch.replaceTree(t, q"$qual.queryRaw.format".syntax)
    }.asPatch
  }

}
