package fix.v0_8_0

import scalafix.v1._

import scala.meta._

object FixBigQueryDeprecations {
  val JavaString: SymbolMatcher = SymbolMatcher.normalized("java/lang/String")
}

final class FixBigQueryDeprecations extends SemanticRule("FixBigQueryDeprecations") {

  import FixBigQueryDeprecations._

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect { case q"$_.bigQueryTable($head, ..$_)" =>
      head.symbol.info.map(_.signature) match {
        case None =>
          // this can only be a string literal
          Patch.replaceTree(head, q"Table.Spec($head)".syntax)
        case Some(MethodSignature(_, _, TypeRef(_, sym, _))) if JavaString.matches(sym) =>
          Patch.replaceTree(head, q"Table.Spec($head)".syntax)
        case _ =>
          Patch.replaceTree(head, q"Table.Ref($head)".syntax)
      }
    }.asPatch
}
