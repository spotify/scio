package fix
package v0_7_0

import scalafix.v1._
import scala.meta._

class AddMissingImports extends SyntacticRule("AddMissingImports") {
  private val imports =
    scala.collection.mutable.ArrayBuffer.empty[(String, String)]

  object Avro {
    val fns =
      List("objectFile", "avroFile", "typedAvroFile", "protobufFile") ++
      List("saveAsAvroFile", "saveAsTypedAvroFile", "saveAsObjectFile", "saveAsProtobufFile")

    val `import` = importer"com.spotify.scio.avro._"
  }

  object BQ {
    val fns =
      List("bigQuerySelect", "bigQueryTable", "bigQueryTable", "typedBigQuery", "tableRowJsonFile") ++
      List("saveAsBigQuery", "saveAsBigQuery", "saveAsTypedBigQuery", "saveAsTypedBigQuery", "saveAsTableRowJsonFile")

    val `import` = importer"com.spotify.scio.bigquery._"
  }

  // Check that the package is not imported multiple times in the same file
  private def addImport(p: Position, i: Importer) = {
    val Importer(s) = i
    val Input.VirtualFile(path, _) = p.input

    val t = (s.toString, path)
    if(!imports.contains(t)) {
      imports += t
      Patch.addGlobalImport(i)
    } else Patch.empty
  }

  override def fix(implicit doc: SyntacticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Name(n) if Avro.fns.contains(n) =>
        addImport(t.pos, Avro.`import`)
      case t @ Term.Name(n) if BQ.fns.contains(n) =>
        addImport(t.pos, BQ.`import`)
    }.asPatch
  }
}
