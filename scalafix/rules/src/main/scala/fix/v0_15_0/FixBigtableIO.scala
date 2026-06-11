package fix.v0_15_0

import scalafix.v1._
import scala.meta._

object FixBigtableIO {
  val BigtableMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/bigtable/syntax/ScioContextOps#bigtable()."
  )
  val btOptionsImport = importer"com.spotify.scio.bigtable.BTOptions"
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
              val projectId = params(0)
              val instanceId = params(1)
              val tableId = params(2)
              val rest = params.drop(3)
              val namedArgs = rest.zipWithIndex.map { case (arg, i) =>
                i match {
                  case 0 => q"keyRanges = $arg"
                  case 1 => q"rowFilter = $arg"
                }
              }
              val newParams = List(q"BTOptions($projectId, $instanceId)", tableId) ++ namedArgs
              Patch.replaceTree(t, q"$expr(..$newParams)".syntax) +
                Patch.addGlobalImport(btOptionsImport)
          }
          .getOrElse(Patch.empty)
    }.asPatch
  }
}
