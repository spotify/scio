package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixBqSaveAsTable extends SemanticRule("FixBqSaveAsTable") {
  private val scoll = "com/spotify/scio/values/SCollection#"
  private val methodName =
    "com.spotify.scio.extra.bigquery.syntax.AvroToBigQuerySCollectionOps.saveAvroAsBigQuery"

  override def fix(implicit doc: SemanticDocument): Patch = {
    val patches =
      doc.tree
        .collect {
          case a @ Term.Apply(fun, params) =>
            if (fun.symbol.normalized.toString.contains(methodName)) {
              fun match {
                case Term.Select(qual, name) =>
                  name match {
                    case Term.Name("saveAvroAsBigQuery") if expectedType(qual, scoll) =>
                      val paramsUpdated =
                        params.zipWithIndex
                          .map { case (param, index) =>
                            index match {
                              // table is always the first param and without default value
                              case 0 =>
                                param match {
                                  case Term.Assign((_, value)) =>
                                    q"table = Table.Ref(${value})"
                                  case _ =>
                                    q"Table.Ref($param)"
                                }
                              case _ =>
                                param match {
                                  case Term.Assign((name, value)) =>
                                    // parameter name has changes from `avroSchema` to `schema`
                                    if (name.toString == "avroSchema") {
                                      q"schema = toTableSchema($value)"
                                    } else {
                                      param
                                    }
                                  case _ =>
                                    // if not a named param, `avroSchema` param should come second
                                    if (index == 1) {
                                      q"toTableSchema($param)"
                                    } else {
                                      param
                                    }
                                }
                            }
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
          case _ => Patch.empty
        }
        .filter(_ != Patch.empty)

    // in case the import is the only change, drop the entire patch
    if (!patches.isEmpty) {
      patches ++ List(
        Patch.addGlobalImport(importer"com.spotify.scio.bigquery._"),
        Patch.addGlobalImport(
          importer"com.spotify.scio.extra.bigquery.AvroConverters.toTableSchema"
        )
      )
    } else {
      patches
    }

  }.asPatch

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
