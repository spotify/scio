package fix.v0_8_0

import scalafix.v1._

import scala.meta._

final class FixTensorflow extends SemanticRule("FixTensorflow") {
  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect { case t @ q"$qual.saveAsTfExampleFile" =>
      Patch.replaceTree(t, q"$qual.saveAsTfRecordFile".syntax)
    }.asPatch
}
