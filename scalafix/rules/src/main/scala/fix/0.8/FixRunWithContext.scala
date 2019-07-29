package fix
package v0_8_0

import scalafix.v1._
import scala.meta._

class FixRunWithContext extends SyntacticRule("FixRunWithContext") {

  private def fixSubtree(name: String)(doc: SyntacticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Select(Term.Name(`name`), Term.Name("isCompleted")) =>
        Patch.empty
      case t @ Term.Select(Term.Name(`name`), Term.Name("state")) =>
        Patch.empty
      case t @ Term.Select(Term.Name(`name`), x) =>
        Patch.replaceTree(t, s"$name.waitUntilFinish.$x")
      case t @ Term.Apply(tn, ps) if ps.find(x => x.toString == name).isDefined =>
        val ps2 =
          ps.map {
            case t if t.toString == name => q"$t.waitUntilFinish"
            case t => t
          }
        Patch.replaceTree(t, Term.Apply(tn, ps2).toString)
    }.asPatch
  }

  override def fix(implicit doc: SyntacticDocument): Patch = {
    doc.tree.collect {
      // Convert val`x` of type ScioExecutionContext to ScioResult
      // when `x` is passed to a method oo
      // if a method is called on `x` (except `isCompleted and `state`)
      case t @ q"val ${x} = runWithContext(${body})" =>
        val children = t.parent.toList.flatMap(_.children).filterNot(_ == t)
        val name = x.toString()
        children.map { c =>
          fixSubtree(name)(SyntacticDocument.fromTree(c))
        }.asPatch
      // Convert ScioExecutionContext to ScioResult in methods that return a ScioResult
      case t @ q"def $fn(..$ps): ScioResult = {$body}" =>
        Patch.addRight(t, ".waitUntilFinish")
      case t =>
        Patch.empty
    }.asPatch
  }
}
