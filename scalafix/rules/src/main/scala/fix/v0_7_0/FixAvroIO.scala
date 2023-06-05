package fix.v0_7_0

import scalafix.v1._

import scala.meta._

object FixAvroIO {
  object IO {
    val `import` = importer"com.spotify.scio.io._"
  }

  object Avro {
    val `import` = importer"com.spotify.scio.avro._"
  }

  object BQ {
    val `import` = importer"com.spotify.scio.bigquery._"
  }

  val JobTestBuilder: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/testing/JobTest.Builder")
}

class FixAvroIO extends SemanticRule("FixAvroIO") {

  import FixAvroIO._

  private def isJobTestBuilder(t: Term)(implicit doc: SemanticDocument): Boolean =
    t.symbol.info.map(_.signature) match {
      case Some(MethodSignature(_, _, TypeRef(_, sym, _))) => JobTestBuilder.matches(sym)
      case _                                               => false
    }

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case q"$expr.input(..$params)" if isJobTestBuilder(expr) =>
        params.toList match {
          case (io @ q"AvroIO($path)") :: value :: Nil =>
            val tparam = value match {
              case _: Term.Name =>
                // with Term.Name we do not have to resolve the returned type
                // we have Iterable[$t]
                val MethodSignature(_, _, TypeRef(_, _, TypeRef(_, t, _) :: Nil)) =
                  value.symbol.info.get.signature
                Type.Name(t.displayName) // may require import, but best effort
              case _ =>
                // type resolution is too complex, fallback to GenericRecord
                // https://scalacenter.github.io/scalafix/docs/developers/semantic-type.html#lookup-type-of-a-term
                t"GenericRecord"
            }
            Patch.replaceTree(io, q"AvroIO[$tparam]($path)".syntax) +
              Patch.addGlobalImport(Avro.`import`)
          case _ =>
            Patch.empty
        }
      // fix IO imports
      case importer"com.spotify.scio.testing.{..$imps}" =>
        imps.collect {
          case i @ importee"TextIO" =>
            Patch.removeImportee(i) + Patch.addGlobalImport(IO.`import`)
          case i @ importee"AvroIO" =>
            Patch.removeImportee(i) + Patch.addGlobalImport(Avro.`import`)
          case i @ importee"BigQueryIO" =>
            Patch.removeImportee(i) + Patch.addGlobalImport(BQ.`import`)
          case i @ importee"PubsubIO" =>
            Patch.removeImportee(i) + Patch.addGlobalImport(IO.`import`)
        }.asPatch
    }.asPatch
}
