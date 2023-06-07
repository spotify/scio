package fix.v0_8_0

import scalafix.v1._

import scala.meta._

object FixScioIO {
  val ScioIO: SymbolMatcher = SymbolMatcher.normalized("com/spotify/scio/io/ScioIO")
}

final class FixScioIO extends SemanticRule("FixScioIO") {

  import FixScioIO._

  // Check that the method is a member of an implementation of ScioIO
  private def isScioIOWrite(writeFn: Term.Name)(implicit doc: SemanticDocument): Boolean = {
    val ClassSignature(_, parents, _, _) = writeFn.symbol.owner.info.get.signature
    parents.exists {
      case TypeRef(_, s, _) => ScioIO.matches(s)
      case _                => false
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      // fix return type (No Future 🤘)
      case t @ q"""..$mods def $writeFn(
                      $sName: SCollection[$sTpe],
                      $psName: $psTpe): $ret = $impl""" if isScioIOWrite(writeFn) =>
        ret.collect { case r @ t"Future[$ts]" => Patch.replaceTree(r, ts.syntax) }.asPatch
    }.asPatch
}
