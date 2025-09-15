package fix.v0_8_0

import scalafix.v1._

import scala.meta._
import scala.meta.contrib._

final class FixRunWithContext extends SemanticRule("FixRunWithContext") {

  private def fixSubtree(ctx: Term.Name)(tree: Tree): Patch =
    tree.collect {
      case q"$qual.isCompleted" if qual.isEqual(ctx) =>
        Patch.empty
      case q"$qual.state" if qual.isEqual(ctx) =>
        Patch.empty
      case t @ q"$qual.$name" if qual.isEqual(ctx) =>
        Patch.replaceTree(t, s"$ctx.waitUntilFinish().$name")
      case q"$fn(..$params)" if params.exists(_.isEqual(ctx)) =>
        params.collect {
          case p if p.isEqual(ctx) =>
            Patch.replaceTree(p, q"$p.waitUntilFinish()".syntax)
        }.asPatch
    }.asPatch

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ q"val $x = runWithContext($_)" =>
        // Convert val`x` of type ScioExecutionContext to ScioResult
        // when `x` is passed to a method oo
        // if a method is called on `x` (except `isCompleted and `state`)
        val scope = t.parent.toList.flatMap(_.children).filterNot(_ == t)
        val Pat.Var(name) = x
        scope.map(fixSubtree(name)).asPatch
      case t @ q"def $fn(..$ps): ScioResult = {$_}" =>
        // Convert ScioExecutionContext to ScioResult in methods that return a ScioResult
        Patch.addRight(t, ".waitUntilFinish()")
    }.asPatch
}
