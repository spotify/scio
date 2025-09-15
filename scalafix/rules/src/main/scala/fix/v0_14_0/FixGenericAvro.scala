package fix.v0_14_0

import scalafix.v1._

import scala.meta._

object FixGenericAvro {
  private val AvroGenericSaveMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/avro/syntax/GenericRecordSCollectionOps#saveAsAvroFile"
  )
}

class FixGenericAvro extends SemanticRule("FixGenericAvro") {
  import FixGenericAvro._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$fn(..$params)" if AvroGenericSaveMatcher.matches(fn.symbol) =>
        params.toList match {
          case _ :: q"$_ = $_" :: _ =>
            // 2nd params is already named
            Patch.empty
          case path :: numShards :: (next @ q"$name = $_") :: tail if name.syntax != "schema" =>
            // 3rd param is named but not schema
            val named = path :: q"numShards = $numShards" :: next :: tail
            Patch.replaceTree(t, q"$fn(..$named)".syntax)
          case path :: numShards :: schema :: tail =>
            val reordered = path :: schema :: numShards :: tail
            Patch.replaceTree(t, q"$fn(..$reordered)".syntax)
          case _ =>
            Patch.empty
        }
    }.asPatch
  }
}
