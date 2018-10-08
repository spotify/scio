package fix
package v0_7_0

import scalafix.v1._
import scala.meta._

class RewriteSysProp extends SyntacticRule("RewriteSysProp") {

  private val imports =
    scala.collection.mutable.ArrayBuffer.empty[(String, String)]

  private def addImport(p: Position, i: Importer) = {
    val Importer(s) = i
    val Input.VirtualFile(path, _) = p.input

    val t = (s.toString, path)
    if(!imports.contains(t)) {
      imports += t
      Patch.addGlobalImport(i)
    } else Patch.empty
  }

  private def toCamelCase(s: String) =
    s.split("_").map(_.toLowerCase.capitalize).mkString

  object NamedSysProp {
    def unapply(s: Term) =
      s match {
        case Term.Apply(Term.Select(Term.Name("sys"), Term.Name("props")),
          List(Term.Select(Term.Name(clazz), Term.Name(key)))) =>
          Option((clazz, key))
        case _ => None
      }
  }

  object StringSysProp {
    def unapply(s: Term) =
      s match {
        case Term.Apply(Term.Select(Term.Name("sys"), Term.Name("props")), List(Lit.String(key))) =>
          Option(key)
        case _ => None
      }
  }

  override def fix(implicit doc: SyntacticDocument): Patch = {
    doc.tree.collect {
      // BigQuery
      case t @ NamedSysProp("BigQueryClient", key) =>
        val pname = Term.Name(toCamelCase(key.replaceAll("_KEY", "")))
        addImport(t.pos, importer"com.spotify.scio.bigquery.BigQuerySysProps") +
        Patch.replaceTree(t, q"BigQuerySysProps.${pname}.value".toString)
      // Sys props
      case t @ StringSysProp("project") =>
        addImport(t.pos, importer"com.spotify.scio.CoreSysProps") +
        Patch.replaceTree(t, q"CoreSysProps.Project.value".toString)
      case t @ StringSysProp("java.home") =>
        addImport(t.pos, importer"com.spotify.scio.CoreSysProps") +
        Patch.replaceTree(t, q"CoreSysProps.Home.value".toString)
      case t @ StringSysProp("java.io.tmpdir") =>
        addImport(t.pos, importer"com.spotify.scio.CoreSysProps") +
        Patch.replaceTree(t, q"CoreSysProps.TmpDir.value".toString)
      case t @ StringSysProp("user.name") =>
        addImport(t.pos, importer"com.spotify.scio.CoreSysProps") +
        Patch.replaceTree(t, q"CoreSysProps.User.value".toString)
      case t @ StringSysProp("user.dir") =>
        addImport(t.pos, importer"com.spotify.scio.CoreSysProps") +
        Patch.replaceTree(t, q"CoreSysProps.UserDir.value".toString)
      // AvroSysProps
      case t @ StringSysProp("avro.types.debug") =>
        addImport(t.pos, importer"com.spotify.scio.avro.AvroSysProps") +
        Patch.replaceTree(t, q"AvroSysProps.Debug.value".toString)
      case t @ StringSysProp("avro.plugin.disable.dump") =>
        addImport(t.pos, importer"com.spotify.scio.avro.AvroSysProps") +
        Patch.replaceTree(t, q"AvroSysProps.DisableDump.value".toString)
      case t @ StringSysProp("avro.class.cache.directory") =>
        addImport(t.pos, importer"com.spotify.scio.avro.AvroSysProps") +
        Patch.replaceTree(t, q"AvroSysProps.CacheDirectory.value".toString)
      // Taps
      case t @ NamedSysProp("Taps", "ALGORITHM_KEY") =>
        addImport(t.pos, importer"com.spotify.scio.io.TapsSysProps") +
        Patch.replaceTree(t, q"TapsSysProps.Algorithm .value".toString)
      case t @ NamedSysProp("Taps", key) =>
        val pname = Term.Name(toCamelCase(key.replaceAll("_KEY", "")))
        addImport(t.pos, importer"com.spotify.scio.io.TapsSysProps") +
        Patch.replaceTree(t, q"TapsSysProps.${pname}.value".toString)
      case _ =>
        Patch.empty
    }.asPatch
  }
}
