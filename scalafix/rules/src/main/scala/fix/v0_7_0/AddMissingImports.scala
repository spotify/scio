package fix.v0_7_0

import scalafix.v1._
import scala.meta._

object AddMissingImports {

  object Avro {
    val fns = Set(
      "objectFile",
      "avroFile",
      "typedAvroFile",
      "protobufFile",
      // save fns
      "saveAsAvroFile",
      "saveAsTypedAvroFile",
      "saveAsObjectFile",
      "saveAsProtobufFile"
    )

    val `import` = importer"com.spotify.scio.avro._"
  }

  object BQ {
    val fns = Set(
      "bigQuerySelect",
      "bigQueryTable",
      "bigQueryTable",
      "typedBigQuery",
      "tableRowJsonFile",
      // save fns
      "saveAsBigQuery",
      "saveAsBigQuery",
      "saveAsTypedBigQuery",
      "saveAsTypedBigQuery",
      "saveAsTableRowJsonFile"
    )

    val `import` = importer"com.spotify.scio.bigquery._"
  }

}

class AddMissingImports extends SyntacticRule("AddMissingImports") {

  import AddMissingImports._

  override def fix(implicit doc: SyntacticDocument): Patch =
    doc.tree.collect {
      case Term.Name(value) if Avro.fns.contains(value) => Patch.addGlobalImport(Avro.`import`)
      case Term.Name(value) if BQ.fns.contains(value)   => Patch.addGlobalImport(BQ.`import`)
    }.asPatch
}
