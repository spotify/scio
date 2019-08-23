package fix
package v0_7_0

import scalafix.v1._
import scala.meta._

class FixAvroIO extends SemanticRule("FixAvroIO") {

  private val imports =
    scala.collection.mutable.ArrayBuffer.empty[(String, String)]

  // Check that the package is not imported multiple times in the same file
  def addImport(p: Position, i: Importer) = {
    val Importer(s) = i
    val Input.VirtualFile(path, _) = p.input

    val t = (s.toString, path)
    if(!imports.contains(t)) {
      imports += t
      Patch.addGlobalImport(i)
    } else Patch.empty
  }

  val builder = Symbol("com/spotify/scio/testing/JobTest.Builder#")

  private def isJobTest(t: Term)(implicit doc: SemanticDocument) =
    t.symbol.info.map {
      _.signature.asInstanceOf[MethodSignature].returnType match {
        case TypeRef(prefix, `builder`, args) =>
          true
        case t =>
          false
      }
    }.getOrElse(false)

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(
                Term.Select(parent, Term.Name("input")),
                List(Term.Apply(name @ Term.Name("AvroIO"), path), value @ Term.Name(_))
              ) if (isJobTest(parent)) =>
        value.symbol.info.get.signature match {
          case s @ MethodSignature(_, _, TypeRef(_, symbol, List(arg))) =>
            addImport(t.pos, importer"com.spotify.scio.avro._") +
            Patch.addRight(name, s"[${arg}]")
          case _ =>
            Patch.empty
        }
      // https://scalacenter.github.io/scalafix/docs/developers/semantic-type.html#lookup-type-of-a-term
      case t @ Term.Apply(
                Term.Select(parent, Term.Name("input")),
                List(Term.Apply(name @ Term.Name("AvroIO"), path), _)
              ) if (isJobTest(parent)) =>
          addImport(t.pos, importer"com.spotify.scio.avro._") +
          Patch.addRight(name, s"[GenericRecord]")
      // fix IO imports
      case t @ Importer(q"com.spotify.scio.testing", imps) =>
        val imports =
          imps.collect {
            case i @ Importee.Name(Name.Indeterminate("TextIO")) =>
              Patch.removeImportee(i) + addImport(t.pos, importer"com.spotify.scio.io._")
            case i @ Importee.Name(Name.Indeterminate("AvroIO")) =>
              Patch.removeImportee(i) + addImport(t.pos, importer"com.spotify.scio.avro._")
            case i @ Importee.Name(Name.Indeterminate("BigQueryIO")) =>
              Patch.removeImportee(i) + addImport(t.pos, importer"com.spotify.scio.bigquery._")
            case i @ Importee.Name(Name.Indeterminate("PubsubIO")) =>
              Patch.removeImportee(i) + addImport(t.pos, importer"com.spotify.scio.io._")
          }
        imports.foldLeft(Patch.empty)(_ + _)
    }.asPatch
  }

}
