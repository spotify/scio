package fix.v0_14_0

import scalafix.v1._
import scala.meta._

object FixLogicalTypeSupplier {
  val ParquetConfigurationMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/parquet/package/ParquetConfiguration#of"
  )

  val SetClassMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "org/apache/hadoop/conf/Configuration#setClass"
  )

  private val ParquetAvroPrefix = "com/spotify/scio/parquet/avro/syntax"
  private val ParquetAvroReadMatcher = SymbolMatcher.normalized(s"$ParquetAvroPrefix/ScioContextOps#parquetAvroFile")
  private val ParquetAvroWriteMatcher = SymbolMatcher.normalized(s"$ParquetAvroPrefix/SCollectionOps#saveAsParquetAvroFile")
}

class FixLogicalTypeSupplier extends SemanticRule("FixLogicalTypeSupplier") {
  import FixLogicalTypeSupplier._

  private def updateIOArgs(fnArgs: Seq[Term])(implicit doc: SemanticDocument): Seq[Term] = {
    def removeTypeSupplier(confArgs: Seq[Term]): Option[Term] = {
      val newConfiguration = confArgs.filterNot {
        case q"$l -> classOf[LogicalTypeSupplier]" => true
        case q"$l -> classOf[AvroLogicalTypeSupplier]" => true
        case _ => false
      }.toList

      if (newConfiguration.isEmpty) {
        None
      } else {
        Some(q"ParquetConfiguration.of(..$newConfiguration)")
      }
    }

    fnArgs.flatMap {
      case Term.Assign(lhs, q"$fn(..$confArgs)") if ParquetConfigurationMatcher.matches(fn.symbol) =>
        removeTypeSupplier(confArgs).map(c => Term.Assign(lhs, c))
      case q"$fn(..$confArgs)" if ParquetConfigurationMatcher.matches(fn.symbol) =>
        removeTypeSupplier(confArgs)
      case a => Some(a)
    }
  }


  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case method@q"$fn(..$args)"
        if (ParquetAvroReadMatcher.matches(fn.symbol) || ParquetAvroWriteMatcher.matches(fn.symbol)) && args.nonEmpty =>
        val newArgs = updateIOArgs(args).toList
        Patch.replaceTree(method, q"$fn(..$newArgs)".syntax)
      case method@q"$lhs.$fn(..$args)"
        if SetClassMatcher.matches(fn.symbol) && args.collect {
          case q"classOf[LogicalTypeSupplier]" => true
          case q"classOf[AvroLogicalTypeSupplier]" => true
        }.nonEmpty =>
        Patch.removeTokens(method.tokens)
    }.asPatch
  }
}
