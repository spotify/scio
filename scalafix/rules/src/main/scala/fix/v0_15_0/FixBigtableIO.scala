package fix.v0_15_0

import scalafix.v1._
import scala.meta._

object FixBigtableIO {
  val BigtableMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/bigtable/syntax/ScioContextOps#bigtable()."
  )
}

final class FixBigtableIO extends SemanticRule("FixBigtableIO") {
  import FixBigtableIO._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$expr(..$params)" if BigtableMatcher.matches(expr.symbol) =>
        expr.symbol.info
          .map(_.signature)
          .collect {
            case MethodSignature(_, paramLists, _)
                if paramLists.nonEmpty &&
                  paramLists.head.size == 6 &&
                  params.size >= 4 &&
                  params.size < 6 =>
              val missing = paramLists.head.drop(params.size).map { sym =>
                sym.displayName match {
                  case "rowFilter" => q"null"
                  case _           => q"None"
                }
              }
              val newParams = params.toList ++ missing
              Patch.replaceTree(t, q"$expr(..$newParams)".syntax)
          }
          .getOrElse(Patch.empty)
    }.asPatch
  }
}
