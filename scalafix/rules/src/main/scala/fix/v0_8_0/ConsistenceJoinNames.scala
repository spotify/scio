package fix
package v0_8_0

import scalafix.v1._

import scala.meta._

object ConsistenceJoinNames {

  val PairedScolFns = Seq(
    "join",
    "fullOuterJoin",
    "leftOuterJoin",
    "rightOuterJoin",
    "sparseLeftOuterJoin",
    "sparseRightOuterJoin",
    "sparseOuterJoin",
    "cogroup",
    "groupWith",
    "sparseLookup"
  ).map(fn => SymbolMatcher.normalized(s"com/spotify/scio/values/PairSCollectionFunctions#$fn"))

  val PairedHashScolFns = Seq(
    "hashJoin",
    "hashLeftJoin",
    "hashFullOuterJoin",
    "hashIntersectByKey"
  ).map(fn => SymbolMatcher.normalized(s"com/spotify/scio/values/PairHashSCollectionFunctions#$fn"))

  val PairedSkewedScolFns = Set(
    "skewedJoin",
    "skewedLeftJoin",
    "skewedFullOuterJoin"
  ).map(fn =>
    SymbolMatcher.normalized(s"com/spotify/scio/values/PairSkewedSCollectionFunctions#$fn")
  )

  val JoinsFns: SymbolMatcher =
    (PairedScolFns ++ PairedHashScolFns ++ PairedSkewedScolFns).reduce(_ + _)
}

final class ConsistenceJoinNames extends SemanticRule("ConsistenceJoinNames") {

  import ConsistenceJoinNames._

  private def renameNamedArgs(args: List[Term]): List[Term] =
    args.map {
      case q"that = $value"        => q"rhs = $value"
      case q"that1 = $value"       => q"rhs1 = $value"
      case q"that2 = $value"       => q"rhs2 = $value"
      case q"that3 = $value"       => q"rhs3 = $value"
      case q"thatNumKeys = $value" => q"rhsNumKeys = $value"
      case p                       => p
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.$fn(..$args)" if JoinsFns.matches(fn.symbol) =>
        val updatedFn = fn match {
          case Term.Name("hashLeftJoin")    => Term.Name("hashLeftOuterJoin")
          case Term.Name("skewedLeftJoin")  => Term.Name("skewedLeftOuterJoin")
          case Term.Name("sparseOuterJoin") => Term.Name("sparseFullOuterJoin")
          case _                            => fn
        }
        val updatedArgs = renameNamedArgs(args)
        Patch.replaceTree(t, q"$qual.$updatedFn(..$updatedArgs)".syntax)
      case t @ q"$qual.$fn(..$args)" =>
        println(fn.symbol)
        Patch.empty
    }
  }.asPatch
}
