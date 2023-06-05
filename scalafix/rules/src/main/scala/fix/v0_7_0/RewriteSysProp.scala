package fix
package v0_7_0

import scalafix.v1._

import scala.meta._

object RewriteSysProp {
  val KeySuffix = "_KEY"

  object BQ {
    val BigQueryClient: SymbolMatcher =
      SymbolMatcher.normalized("com/spotify/scio/bigquery/BigQueryClient")
    val `import` = importer"com.spotify.scio.bigquery.BigQuerySysProps"
  }

  object Core {
    val `import` = importer"com.spotify.scio.CoreSysProps"
  }

  object Avro {
    val `import` = importer"com.spotify.scio.avro.AvroSysProps"
  }

  object Taps {
    val Taps: SymbolMatcher = SymbolMatcher.normalized("com/spotify/scio/io/Taps")
    val `import` = importer"com.spotify.scio.io.TapsSysProps"
  }

  def toCamelCase(s: String): String =
    s.split("_").map(_.toLowerCase.capitalize).mkString

}

class RewriteSysProp extends SemanticRule("RewriteSysProp") {

  import RewriteSysProp._

  def isKeyTerm(term: Term): Boolean = term match {
    case name: Term.Name => isKey(name)
    case q"$_.$name"     => isKey(name)
    case _               => false
  }

  def isKey(name: Name): Boolean = name.value.endsWith(KeySuffix)

  def fieldName(keyTerm: Term): Term.Name = keyTerm match {
    case name: Term.Name => Term.Name(toCamelCase(name.value.dropRight(KeySuffix.length)))
    case q"$_.$name"     => Term.Name(toCamelCase(name.value.dropRight(KeySuffix.length)))
    case _               => throw new Exception(s"Unsupported property key '$keyTerm'")
  }

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      // CoreSysProps
      case t @ q"""sys.props("project")""" =>
        Patch.addGlobalImport(Core.`import`) +
          Patch.replaceTree(t, q"CoreSysProps.Project.value".syntax)
      case t @ q"""sys.props("java.home")""" =>
        Patch.addGlobalImport(Core.`import`) +
          Patch.replaceTree(t, q"CoreSysProps.Home.value".syntax)
      case t @ q"""sys.props("java.io.tmpdir")""" =>
        Patch.addGlobalImport(Core.`import`) +
          Patch.replaceTree(t, q"CoreSysProps.TmpDir.value".syntax)
      case t @ q"""sys.props("user.name")""" =>
        Patch.addGlobalImport(Core.`import`) +
          Patch.replaceTree(t, q"CoreSysProps.User.value".syntax)
      case t @ q"""sys.props("user.dir")""" =>
        Patch.addGlobalImport(Core.`import`) +
          Patch.replaceTree(t, q"CoreSysProps.UserDir.value".syntax)
      // AvroSysProps
      case t @ q"""sys.props("avro.types.debug")""" =>
        Patch.addGlobalImport(Avro.`import`) +
          Patch.replaceTree(t, q"AvroSysProps.Debug.value".syntax)
      case t @ q"""sys.props("avro.plugin.disable.dump")""" =>
        Patch.addGlobalImport(Avro.`import`) +
          Patch.replaceTree(t, q"AvroSysProps.DisableDump.value".syntax)
      case t @ q"""sys.props("avro.class.cache.directory")""" =>
        Patch.addGlobalImport(Avro.`import`) +
          Patch.replaceTree(t, q"AvroSysProps.CacheDirectory.value".syntax)
      // BigQuerySysProps
      case t @ q"sys.props($key)"
          if BQ.BigQueryClient.matches(key.symbol.owner) && isKeyTerm(key) =>
        val field = fieldName(key)
        Patch.addGlobalImport(BQ.`import`) +
          Patch.replaceTree(t, q"BigQuerySysProps.$field.value".syntax)
      case i @ importee"BigQueryClient" if BQ.BigQueryClient.matches(i.symbol) =>
        Patch.removeImportee(i.asInstanceOf[Importee])
      case importer"com.spotify.scio.bigquery.BigQueryClient.{..$is}" =>
        is.collect {
          case i @ importee"$name" if isKey(name) => Patch.removeImportee(i)
        }.asPatch
      // TapsSysProps
      case t @ q"sys.props($key)" if Taps.Taps.matches(key.symbol.owner) && isKeyTerm(key) =>
        val field = fieldName(key)
        Patch.addGlobalImport(Taps.`import`) +
          Patch.replaceTree(t, q"TapsSysProps.$field.value".syntax)
      case i @ importee"BigQueryClient" =>
        Patch.removeImportee(i.asInstanceOf[Importee])
      case i @ importee"Taps" if Taps.Taps.matches(i.symbol) =>
        Patch.removeImportee(i.asInstanceOf[Importee])
      case importer"$_.Taps.{..$is}" =>
        is.collect {
          case i @ importee"$name" if isKey(name) => Patch.removeImportee(i)
        }.asPatch
    }.asPatch
}
