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

  val JavaClassMatcher: SymbolMatcher = SymbolMatcher.normalized("java/lang/Class")

  private val ParquetAvroPrefix = "com/spotify/scio/parquet/avro"
  val LogicalTypeSupplierMatcher: SymbolMatcher = SymbolMatcher.normalized(
    s"$ParquetAvroPrefix/LogicalTypeSupplier",
    "org/apache/beam/sdk/extensions/smb/AvroLogicalTypeSupplier"
  )

  private val ParquetAvroMatcher = SymbolMatcher.normalized(
    s"$ParquetAvroPrefix/syntax/ScioContextOps#parquetAvroFile",
    s"$ParquetAvroPrefix/syntax/SCollectionOps#saveAsParquetAvroFile"
  )
}

class FixLogicalTypeSupplier extends SemanticRule("FixLogicalTypeSupplier") {
  import FixLogicalTypeSupplier._

  private def isLogicalTypeSupplier(term: Term)(implicit doc: SemanticDocument): Boolean =
    term match {
      case q"classOf[$tpe]" => LogicalTypeSupplierMatcher.matches(tpe.symbol)
      case _ =>
        term.symbol.info
          .map(_.signature)
          .collect { case MethodSignature(_, _, returnedType) => returnedType }
          .collect { case TypeRef(_, sym, tpe :: Nil) if JavaClassMatcher.matches(sym) => tpe }
          .collect { case TypeRef(_, sym, _) => sym }
          .exists(LogicalTypeSupplierMatcher.matches)
    }

  private def parquetConfigurationArgs(
    confArgs: List[Term]
  )(implicit doc: SemanticDocument): List[Term] = confArgs.filterNot {
    case q"($_, $rhs)" => isLogicalTypeSupplier(rhs)
    case q"$_ -> $rhs" => isLogicalTypeSupplier(rhs)
    case _             => false
  }

  private def updateIOArgs(fnArgs: List[Term])(implicit doc: SemanticDocument): List[Term] = {
    fnArgs.flatMap {
      case q"$lhs = $fn(..$confArgs)" if ParquetConfigurationMatcher.matches(fn.symbol) =>
        val filtered = parquetConfigurationArgs(confArgs)
        if (filtered.isEmpty) None else Some(q"$lhs = ParquetConfiguration.of(..$filtered)")
      case q"$fn(..$confArgs)" if ParquetConfigurationMatcher.matches(fn.symbol) =>
        val filtered = parquetConfigurationArgs(confArgs)
        if (filtered.isEmpty) None else Some(q"ParquetConfiguration.of(..$filtered)")
      case a =>
        Some(a)
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case method @ q"$fn(..$args)" if ParquetAvroMatcher.matches(fn.symbol) =>
        val newArgs = updateIOArgs(args)
        Patch.replaceTree(method, q"$fn(..$newArgs)".syntax)
      case method @ q"$_.$fn($_, $theClass, $xface)" if SetClassMatcher.matches(fn.symbol) =>
        if (isLogicalTypeSupplier(theClass) || isLogicalTypeSupplier(xface)) {
          Patch.removeTokens(method.tokens)
        } else {
          Patch.empty
        }
      case importer"com.spotify.scio.parquet.avro.{..$importees}" =>
        importees.collect {
          case i @ importee"LogicalTypeSupplier" => Patch.removeImportee(i)
          case _                                 => Patch.empty
        }.asPatch
      case importer"org.apache.beam.sdk.extensions.smb.{..$importees}" =>
        importees.collect {
          case i @ importee"AvroLogicalTypeSupplier" => Patch.removeImportee(i)
          case _                                     => Patch.empty
        }.asPatch
    }.asPatch
  }
}
