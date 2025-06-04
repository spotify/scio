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

  val OptionMatcher: SymbolMatcher = SymbolMatcher.normalized("scala/Some", "scala/Option")

  private val ParquetAvroPrefix = "com/spotify/scio/parquet/avro"
  val LogicalTypeSupplierMatcher: SymbolMatcher = SymbolMatcher.normalized(
    s"$ParquetAvroPrefix/LogicalTypeSupplier",
    "org/apache/beam/sdk/extensions/smb/AvroLogicalTypeSupplier"
  )
}

class FixLogicalTypeSupplier extends SemanticRule("FixLogicalTypeSupplier") {
  import FixLogicalTypeSupplier._

  private def isLogicalTypeSupplier(term: Term)(implicit doc: SemanticDocument): Boolean =
    term match {
      case q"classOf[$tpe]" => LogicalTypeSupplierMatcher.matches(tpe.symbol)
      case _                =>
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
    def filterArgs(lhsOpt: Option[Term], rhsOption: Boolean, confArgs: List[Term]): Option[Term] = {
      val filtered = parquetConfigurationArgs(confArgs)
      (lhsOpt, rhsOption, filtered.isEmpty) match {
        case (_, _, true)              => None
        case (Some(lhs), true, false)  => Some(q"$lhs = Some(ParquetConfiguration.of(..$filtered))")
        case (Some(lhs), false, false) => Some(q"$lhs = ParquetConfiguration.of(..$filtered)")
        case (None, true, false)       => Some(q"Some(ParquetConfiguration.of(..$filtered))")
        case (None, false, false)      => Some(q"ParquetConfiguration.of(..$filtered)")
      }
    }

    fnArgs.flatMap {
      case q"$lhs = $fn(..$confArgs)" if ParquetConfigurationMatcher.matches(fn.symbol) =>
        filterArgs(Some(lhs), false, confArgs)
      case q"$fn(..$confArgs)" if ParquetConfigurationMatcher.matches(fn.symbol) =>
        filterArgs(None, false, confArgs)
      case q"$lhs = $maybeOpt($fn(..$confArgs))"
          if ParquetConfigurationMatcher.matches(fn.symbol) && OptionMatcher.matches(maybeOpt) =>
        filterArgs(Some(lhs), true, confArgs)
      case q"$maybeOpt($fn(..$confArgs))"
          if ParquetConfigurationMatcher.matches(fn.symbol) && OptionMatcher.matches(maybeOpt) =>
        filterArgs(None, true, confArgs)
      case a =>
        Some(a)
    }
  }

  private def containsConfArg(args: Seq[Term])(implicit doc: SemanticDocument): Boolean = {
    def isParquetConf(term: Term): Boolean = ParquetConfigurationMatcher.matches(term.symbol)

    args.exists {
      case q"$_ = $fn(..$args)" if isParquetConf(fn)       => true
      case q"$fn(..$args)" if isParquetConf(fn)            => true
      case q"$_ = Some($fn(..$args))" if isParquetConf(fn) => true
      case q"Some($fn(..$args))" if isParquetConf(fn)      => true
      case _                                               => false
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case method @ q"$coll.$fn(..$args)" if containsConfArg(args) =>
        val newArgs = updateIOArgs(args)
        Patch.replaceTree(method, q"$coll.$fn(..$newArgs)".syntax)
      case method @ q"$coll.$fn[$exprs](..$args)" if containsConfArg(args) =>
        val newArgs = updateIOArgs(args)
        Patch.replaceTree(method, q"$coll.$fn[$exprs](..$newArgs)".syntax)
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
