package fix.v0_14_0

import fix.common.Common
import scalafix.v1._

import scala.meta._

object FixAvroCoder {
  val avroImport = importer"com.spotify.scio.avro._"
  val CoderMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/coders/Coder"
  )
  val AvroCoderPath: String = "com/spotify/scio/coders/instances/AvroCoders"
  val AvroCoderMatcher: SymbolMatcher = SymbolMatcher.normalized(
    AvroCoderPath + "#avroGenericRecordCoder",
    AvroCoderPath + "#avroSpecificRecordCoder",
    AvroCoderPath + "#avroSpecificFixedCoder"
  )
  val AvroMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "org/apache/avro/specific/SpecificRecord",
    "org/apache/avro/specific/SpecificFixed",
    "org/apache/avro/generic/GenericRecord"
  )

  def isAvroType(sym: Symbol)(implicit sd: SemanticDocument): Boolean =
    AvroMatcher.matches(sym) || Common.hasParentClass(sym, AvroMatcher)
}

class FixAvroCoder extends SemanticRule("FixAvroCoder") {
  import FixAvroCoder._

  override def fix(implicit doc: SemanticDocument): Patch = {
    val usesAvroCoders = doc.tree
      .collect {
        case q"$expr[..$tpesnel]" =>
          // A method call with a type parameter requires an implicit Coder[T] for our type
          Common
            .findBoundedTypes(expr, tpesnel, CoderMatcher)
            .exists(isAvroType)
        case t @ q"..$mods val ..$patsnel: $tpeopt = $expr" =>
          // Coder[T] is a variable type where T is an avro type
          Common
            .findMatchingValTypeParams(t, CoderMatcher)
            .exists(isAvroType)
      }
      .foldLeft(false)(_ || _)
    val avroValuePatch = if (usesAvroCoders) Patch.addGlobalImport(avroImport) else Patch.empty

    val patches = doc.tree.collect {
      case importer"com.spotify.scio.coders.Coder.{..$imps}" =>
        // fix direct import from Coder
        imps.collect {
          case i @ (importee"avroGenericRecordCoder" | importee"avroSpecificRecordCoder" |
              importee"avroSpecificFixedCoder") =>
            Patch.removeImportee(i) + Patch.addGlobalImport(avroImport)
        }.asPatch
      case t @ q"$obj.$fn" if AvroCoderMatcher.matches(fn.symbol) =>
        // fix direct usage of Coder.avro*
        Patch.replaceTree(t, q"$fn".syntax) + Patch.addGlobalImport(avroImport)
    }.asPatch

    patches + avroValuePatch
  }
}
