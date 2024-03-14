package fix.v0_14_0

import scalafix.v1._
import scala.meta._

object FixQuery {
  val HasQueryMatcher: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/bigquery/types/BigQueryType/HasQuery#query")
}

class FixQuery extends SemanticRule("FixQuery") {

  import FixQuery._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.$fn" if HasQueryMatcher.matches(fn) =>
        Patch.replaceTree(t, q"$qual.queryRaw".syntax)
    }.asPatch
  }

}
