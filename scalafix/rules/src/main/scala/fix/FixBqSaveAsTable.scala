package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixBqSaveAsTable extends SemanticRule("FixBqSaveAsTable") {
  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect { case a@Term.Apply(fun, head :: tail) =>
      fun match {
        case Term.Select(qual, name) =>
          name match {
            case t@Term.Name("saveAvroAsBigQuery") => //TODO(farzad) better match?
              // the rest of args should be named because the parameter order has changed
              if (tail.exists(!_.toString.contains("=")) ||
                  // `avroSchema` doesn't exist in the list of the new method
                  tail.exists(_.toString.contains("avroSchema"))
              ) {
                Patch.empty // not possible to fix, leave it to `LintBqSaveAsTable`
              } else {
                val headParam = if (head.toString.contains("=")) {
                  s"table = Table.Ref(${head.toString.split("=").last.trim})"
                } else {
                  s"Table.Ref($head)"
                }
                val allArgs = (headParam :: tail).mkString(", ")
                // TODO(farzad): remove ""
                Patch.replaceTree(a, s"$qual.saveAsBigQueryTable($allArgs)")
              }
            case _ =>
              Patch.empty
          }
      }
    }.asPatch
  }
}
