package fix.v0_14_0

import scalafix.v1.{MethodSignature, _}

import scala.meta._
import scala.meta.contrib._

object FixAvroSchemasPackage {
  val `import` = importer"com.spotify.scio.avro._"
}

class FixAvroSchemasPackage extends SemanticRule("FixAvroSchemasPackage") {
  import FixAvroSchemasPackage._

  override def fix(implicit doc: SemanticDocument): Patch = {
    /// import com.spotify.scio.schemas._
    // if avro schemas used then add
    // import com.spotify.scio.avro.schemas._
    // TODO
    Patch.empty
  }
}
