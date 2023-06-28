package fix.v0_13_0

import scalafix.v1.{MethodSignature, _}

import scala.meta._
import scala.meta.contrib._

object FixTfParameter {
  private val Ops = "com/spotify/scio/tensorflow/syntax/PredictSCollectionOps"
  val PredictMatcher: SymbolMatcher = SymbolMatcher.normalized(Ops + "#predict") +
    SymbolMatcher.normalized(Ops + "#predictWithSigDef")
}

class FixTfParameter extends SemanticRule("FixTfParameter") {
  import FixTfParameter._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$fn[..$tParams]" if PredictMatcher.matches(fn.symbol) && tParams.length == 2 =>
        val newTParam = tParams.toList.head
        Patch.replaceTree(t, q"$fn[$newTParam]".syntax)
    }.asPatch
  }
}
