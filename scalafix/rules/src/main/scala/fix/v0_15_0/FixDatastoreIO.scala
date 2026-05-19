package fix.v0_15_0

import scalafix.v1._
import scala.meta._

object FixDatastoreIO {
  val entityImport = importer"com.google.datastore.v1.Entity"
  val DatastoreIOMatcher: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/datastore/DatastoreIO")
}

final class FixDatastoreIO extends SemanticRule("FixDatastoreIO") {
  import FixDatastoreIO._

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      // Match DatastoreIO(...) calls and add the Entity type parameter
      case t @ q"$expr(..$params)" if DatastoreIOMatcher.matches(expr.symbol) =>
        val entityType = t"Entity"
        Patch.replaceTree(
          t,
          q"$expr[$entityType](..$params)".syntax
        ) + Patch.addGlobalImport(entityImport)
    }.asPatch
  }
}
