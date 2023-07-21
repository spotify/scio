package fix.v0_14_0

import scalafix.v1._

import scala.meta._

object FixDynamicAvro {
  private val AvroDynamicImport = importer"com.spotify.scio.avro.dynamic._"
  private val Prefix = "com/spotify/scio/io/dynamic/syntax/"

  private val AvroDynamicMatcher = SymbolMatcher.normalized(
    Prefix + "DynamicGenericRecordSCollectionOps#saveAsDynamicAvroFile",
    Prefix + "DynamicProtobufSCollectionOps#saveAsDynamicProtobufFile",
    Prefix + "DynamicSpecificRecordSCollectionOps#saveAsDynamicAvroFile"
  )
}

class FixDynamicAvro extends SemanticRule("FixDynamicAvro") {
  import FixDynamicAvro._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case q"$fn" if AvroDynamicMatcher.matches(fn.symbol) =>
        Patch.addGlobalImport(AvroDynamicImport)
    }.asPatch
  }
}
