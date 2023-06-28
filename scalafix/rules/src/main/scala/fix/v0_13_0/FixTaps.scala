package fix.v0_13_0

import scalafix.v1._

import scala.meta._

object FixTaps {
  private val scio = "com.spotify.scio."
  private val avro = scio + "avro."

  val ParquetParam = SymbolMatcher.normalized(scio + "parquet.types.ParquetTypeIO.ReadParam")

  val TextTap = SymbolMatcher.normalized(scio + "io.TextTap")
  val TFTap = SymbolMatcher.normalized(scio + "tensorflow.TFRecordFileTap")
  val AvroTaps = SymbolMatcher.normalized(avro + "SpecificRecordTap") +
    SymbolMatcher.normalized(avro + "ObjectFileTap")
  val GRTaps = SymbolMatcher.normalized(avro + "GenericRecordTap") +
    SymbolMatcher.normalized(avro + "GenericRecordParseTap")

  val AvroIOImport = importer"com.spotify.scio.avro.AvroIO"
  val TextIOImport = importer"com.spotify.scio.io.TextIO"
  val TFRecordIOImport = importer"com.spotify.scio.tensorflow.TFRecordIO"

  def addReadParam(t: Tree, clazz: Term, optTParam: Option[Type], params: Seq[Term], repl: Term) = {
    val updated = params.toList ++ List(repl)
    optTParam match {
      case None         => Patch.replaceTree(t, q"$clazz(..$updated)".syntax)
      case Some(tParam) => Patch.replaceTree(t, q"$clazz[$tParam](..$updated)".syntax)
    }
  }
}

class FixTaps extends SemanticRule("FixTaps") {
  import FixTaps._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$clazz[$tParam]" if ParquetParam.matches(clazz.symbol) =>
        // drop type parameter for ParquetTypeIO.ReadParam
        Patch.replaceTree(t, q"$clazz".syntax)
      case t @ q"$clazz[$tParam](..$params)"
          if AvroTaps.matches(clazz.symbol) && params.length == 1 =>
        addReadParam(t, clazz, Some(tParam), params, q"AvroIO.ReadParam()") +
          Patch.addGlobalImport(AvroIOImport)
      case t @ q"$clazz(..$params)" =>
        if (GRTaps.matches(clazz.symbol) && params.length == 2) {
          addReadParam(t, clazz, None, params, q"AvroIO.ReadParam()") +
            Patch.addGlobalImport(AvroIOImport)
        } else if (TextTap.matches(clazz.symbol) && params.length == 1) {
          addReadParam(t, clazz, None, params, q"TextIO.ReadParam()") +
            Patch.addGlobalImport(TextIOImport)
        } else if (TFTap.matches(clazz.symbol) && params.length == 1) {
          addReadParam(t, clazz, None, params, q"TFRecordIO.ReadParam()") +
            Patch.addGlobalImport(TFRecordIOImport)
        } else {
          Patch.empty
        }
    }.asPatch
  }
}
