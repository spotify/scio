package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixBqSaveAsTable extends SemanticRule("FixBqSaveAsTable") {
  private val scoll = "com/spotify/scio/values/SCollection#"
  private val methodName =
    "com.spotify.scio.extra.bigquery.syntax.AvroToBigQuerySCollectionOps.saveAvroAsBigQuery"

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case a @ Term.Apply(fun, params) =>
        if (fun.symbol.normalized.toString.contains(methodName)) {
          fun match {
            case Term.Select(qual, name) =>
              name match {
                case Term.Name("saveAvroAsBigQuery") if expectedType(qual, scoll) =>
                  val paramsUpdated =
                    params
                      .map(_.toString)
                      .zipWithIndex
                      .foldLeft(List[String]()) { case (accumulator, (param, index)) =>
                        accumulator :+ (
                          index match {
                            // table is always the first param and without default value
                            case 0 =>
                              if (param.contains("=")) {
                                s"table = Table.Ref(${param.split("=").last.trim})"
                              } else {
                                s"Table.Ref($param)"
                              }

                            // if not named, `avroSchema` param should come second
                            case 1 if !param.contains("=") => s"toTableSchema($param)"

                            // parameter name has changes from `avroSchema` to `schema`
                            case _ if param.startsWith("avroSchema") && param.contains("=") =>
                              s"schema = toTableSchema(${param.split("=").last.trim})"

                            // everything else can be kept as is
                            case _ => param
                          }
                        )
                      }
                      .mkString(", ")

                  Patch.replaceTree(a, s"$qual.saveAsBigQueryTable($paramsUpdated)")

                case _ =>
                  Patch.empty
              }
            case _ =>
              Patch.empty
          }
        } else {
          Patch.empty
        }
      case Importer(q"com.spotify.scio.extra.bigquery", imps) =>
        Patch.removeImportee(imps.head) +
          Patch.addGlobalImport(importer"com.spotify.scio.bigquery._") +
          Patch.addGlobalImport(
            importer"com.spotify.scio.extra.bigquery.AvroConverters.toTableSchema"
          )
      case _ => Patch.empty
    }.asPatch
  }

  private def expectedType(qual: Term, typStr: String)(implicit doc: SemanticDocument): Boolean =
    qual.symbol.info.get.signature match {
      case MethodSignature(_, _, TypeRef(_, typ, _)) =>
        SymbolMatcher.exact(typStr).matches(typ)
      case ValueSignature(AnnotatedType(_, TypeRef(_, typ, _))) =>
        SymbolMatcher.exact(typStr).matches(typ)
      case ValueSignature(TypeRef(_, typ, _)) =>
        SymbolMatcher.exact(scoll).matches(typ)
      case t =>
        false
    }
}
