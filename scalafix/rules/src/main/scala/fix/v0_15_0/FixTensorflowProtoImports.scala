package fix.v0_15_0

import scalafix.v1._
import scala.meta._

final class FixTensorflowProtoImports extends SemanticRule("FixTensorflowProtoImports") {
  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case i: Importer if i.ref.syntax.startsWith("org.tensorflow.proto.example") =>
        val newRef = i.ref.syntax.replace("org.tensorflow.proto.example", "org.tensorflow.proto")
        Patch.replaceTree(i.ref, newRef)
    }.asPatch
  }
}
