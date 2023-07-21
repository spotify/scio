package fix.v0_14_0

import fix.common.Common
import scalafix.v1._

import scala.meta._

object FixAvroSchemasPackage {
  val avroImport = importer"com.spotify.scio.avro.schemas._"
  val fromAvroMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/schemas/instances/AvroInstances#fromAvroSchema"
  )
  val SchemaMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/schemas/Schema"
  )
  val SpecificRecordMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "org/apache/avro/specific/SpecificRecord"
  )

  def isSpecificRecord(sym: Symbol)(implicit sd: SemanticDocument) =
    Common.hasParentClass(sym, SpecificRecordMatcher)

  def avroImportee(imps: Seq[Importee]): Patch = {
    imps.collect { case i @ importee"fromAvroSchema" =>
      Patch.removeImportee(i) + Patch.addGlobalImport(avroImport)
    }.asPatch
  }
}

class FixAvroSchemasPackage extends SemanticRule("FixAvroSchemasPackage") {
  import FixAvroSchemasPackage._

  override def fix(implicit doc: SemanticDocument): Patch = {
    val hasAvroSomewhere: Boolean = doc.tree
      .collect {
        case q"$expr[..$tpesnel]" =>
          // A method call with a type parameter requires an implicit Schema[T] for our type
          Common
            .findBoundedTypes(expr, tpesnel, SchemaMatcher)
            .exists(isSpecificRecord)
        case t @ q"..$mods val ..$patsnel: $tpeopt = $expr" =>
          // Schema[T] is a variable type where T <: SpecificRecord
          Common
            .findMatchingValTypeParams(t, SchemaMatcher)
            .exists(isSpecificRecord)
      }
      .foldLeft(false)(_ || _)
    val avroValuePatch = if (hasAvroSomewhere) Patch.addGlobalImport(avroImport) else Patch.empty

    val patches = doc.tree.collect {
      case importer"com.spotify.scio.schemas.Schema.{..$imps}" =>
        // fix direct import of fromAvroSchema
        avroImportee(imps)
      case importer"com.spotify.scio.schemas.instances.AvroInstances.{..$imps}" =>
        // fix direct import of fromAvroSchema
        avroImportee(imps)
      case t @ q"$obj.$fn" if fromAvroMatcher.matches(fn.symbol) =>
        // fix direct usage of Schema.fromAvroSchema
        Patch.replaceTree(t, q"$fn".syntax) + Patch.addGlobalImport(avroImport)
    }.asPatch

    patches + avroValuePatch
  }
}
