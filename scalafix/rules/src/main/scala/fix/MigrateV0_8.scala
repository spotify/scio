package fix
package v0_8_0

import scalafix.v1._
import scala.meta._
import scala.meta.contrib._

final class FixRunWithContext extends SemanticRule("FixRunWithContext") {

  private def fixSubtree(ctx: Term.Name)(tree: Tree): Patch =
    tree.collect {
      case q"$qual.isCompleted" if qual.isEqual(ctx) =>
        Patch.empty
      case q"$qual.state" if qual.isEqual(ctx) =>
        Patch.empty
      case t @ q"$qual.$name" if qual.isEqual(ctx) =>
        Patch.replaceTree(t, s"$ctx.waitUntilFinish().$name")
      case q"$fn(..$params)" if params.exists(_.isEqual(ctx)) =>
        params.collect {
          case p if p.isEqual(ctx) =>
            Patch.replaceTree(p, q"$p.waitUntilFinish()".syntax)
        }.asPatch
    }.asPatch

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ q"val $x = runWithContext($_)" =>
        // Convert val`x` of type ScioExecutionContext to ScioResult
        // when `x` is passed to a method oo
        // if a method is called on `x` (except `isCompleted and `state`)
        val scope = t.parent.toList.flatMap(_.children).filterNot(_ == t)
        val Pat.Var(name) = x
        scope.map(fixSubtree(name)).asPatch
      case t @ q"def $fn(..$ps): ScioResult = {$_}" =>
        // Convert ScioExecutionContext to ScioResult in methods that return a ScioResult
        Patch.addRight(t, ".waitUntilFinish()")
    }.asPatch
}

object FixScioIO {
  val ScioIO: SymbolMatcher = SymbolMatcher.normalized("com/spotify/scio/io/ScioIO")
}

final class FixScioIO extends SemanticRule("FixScioIO") {

  import FixScioIO._

  // Check that the method is a member of an implementation of ScioIO
  private def isScioIOWrite(writeFn: Term.Name)(implicit doc: SemanticDocument): Boolean = {
    val ClassSignature(_, parents, _, _) = writeFn.symbol.owner.info.get.signature
    parents.exists {
      case TypeRef(_, s, _) => ScioIO.matches(s)
      case _                => false
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      // fix return type (No Future 🤘)
      case t @ q"""..$mods def $writeFn(
                      $sName: SCollection[$sTpe],
                      $psName: $psTpe): $ret = $impl""" if isScioIOWrite(writeFn) =>
        ret.collect { case r @ t"Future[$ts]" => Patch.replaceTree(r, ts.syntax) }.asPatch
    }.asPatch
}

object FixSyntaxImports {
  object JDBC {
    val fns = List("JdbcScioContext", "JdbcSCollection")
    val `import` = importer"com.spotify.scio.jdbc._"
  }

  object BQ {
    val fns = List("toBigQueryScioContext", "toBigQuerySCollection")
    val `import` = importer"com.spotify.scio.bigquery._"
  }
}

final class FixSyntaxImports extends SemanticRule("FixSyntaxImports") {

  import FixSyntaxImports._

  override def fix(implicit doc: SemanticDocument): Patch = {
    val renameSymbols = Patch.replaceSymbols(
      "com/spotify/scio/transforms/AsyncLookupDoFn." -> "com/spotify/scio/transforms/BaseAsyncLookupDoFn.",
      "com.spotify.scio.jdbc.JdbcScioContext." -> "com.spotify.scio.jdbc.syntax.JdbcScioContextOps."
    )

    doc.tree.collect {
      case i @ importee"$name" if JDBC.fns.contains(name.value) =>
        Patch.removeImportee(i.asInstanceOf[Importee]) + Patch.addGlobalImport(JDBC.`import`)
      case i @ importee"$name" if BQ.fns.contains(name.value) =>
        Patch.removeImportee(i.asInstanceOf[Importee]) + Patch.addGlobalImport(BQ.`import`)
    }.asPatch + renameSymbols
  }
}

object FixContextClose {
  val ScioContextClose = SymbolMatcher.normalized("com/spotify/scio/ScioContext#close")
}

final class FixContextClose extends SemanticRule("FixContextClose") {
  import FixContextClose._

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ q"$qual.$fn()" if ScioContextClose.matches(fn.symbol) =>
        Patch.replaceTree(t, q"$qual.run()".syntax)
    }.asPatch
}

final class FixTensorflow extends SemanticRule("FixTensorflow") {
  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect { case t @ q"$qual.saveAsTfExampleFile" =>
      Patch.replaceTree(t, q"$qual.saveAsTfRecordFile".syntax)
    }.asPatch
}

object FixBigQueryDeprecations {
  val JavaString: SymbolMatcher = SymbolMatcher.normalized("java/lang/String")
}

final class FixBigQueryDeprecations extends SemanticRule("FixBigQueryDeprecations") {

  import FixBigQueryDeprecations._

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect { case q"$_.bigQueryTable($head, ..$_)" =>
      head.symbol.info.map(_.signature) match {
        case None =>
          // this can only be a string literal
          Patch.replaceTree(head, q"Table.Spec($head)".syntax)
        case Some(MethodSignature(_, _, TypeRef(_, sym, _))) if JavaString.matches(sym) =>
          Patch.replaceTree(head, q"Table.Spec($head)".syntax)
        case _ =>
          Patch.replaceTree(head, q"Table.Ref($head)".syntax)
      }
    }.asPatch
}

object ConsistenceJoinNames {

  val PairedScolFns = Seq(
    "join",
    "fullOuterJoin",
    "leftOuterJoin",
    "rightOuterJoin",
    "sparseLeftOuterJoin",
    "sparseRightOuterJoin",
    "sparseOuterJoin",
    "cogroup",
    "groupWith",
    "sparseLookup"
  ).map(fn => SymbolMatcher.normalized(s"com/spotify/scio/values/PairSCollectionFunctions#$fn"))

  val PairedHashScolFns = Seq(
    "hashJoin",
    "hashLeftJoin",
    "hashFullOuterJoin",
    "hashIntersectByKey"
  ).map(fn => SymbolMatcher.normalized(s"com/spotify/scio/values/PairHashSCollectionFunctions#$fn"))

  val PairedSkewedScolFns = Set(
    "skewedJoin",
    "skewedLeftJoin",
    "skewedFullOuterJoin"
  ).map(fn =>
    SymbolMatcher.normalized(s"com/spotify/scio/values/PairSkewedSCollectionFunctions#$fn")
  )

  val JoinsFns: SymbolMatcher =
    (PairedScolFns ++ PairedHashScolFns ++ PairedSkewedScolFns).reduce(_ + _)
}

final class ConsistenceJoinNames extends SemanticRule("ConsistenceJoinNames") {

  import ConsistenceJoinNames._

  private def renameNamedArgs(args: List[Term]): List[Term] =
    args.map {
      case q"that = $value"        => q"rhs = $value"
      case q"that1 = $value"       => q"rhs1 = $value"
      case q"that2 = $value"       => q"rhs2 = $value"
      case q"that3 = $value"       => q"rhs3 = $value"
      case q"thatNumKeys = $value" => q"rhsNumKeys = $value"
      case p                       => p
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$qual.$fn(..$args)" if JoinsFns.matches(fn.symbol) =>
        val updatedFn = fn match {
          case Term.Name("hashLeftJoin")    => Term.Name("hashLeftOuterJoin")
          case Term.Name("skewedLeftJoin")  => Term.Name("skewedLeftOuterJoin")
          case Term.Name("sparseOuterJoin") => Term.Name("sparseFullOuterJoin")
          case _                            => fn
        }
        val updatedArgs = renameNamedArgs(args)
        Patch.replaceTree(t, q"$qual.$updatedFn(..$updatedArgs)".syntax)
      case t @ q"$qual.$fn(..$args)" =>
        println(fn.symbol)
        Patch.empty
    }
  }.asPatch
}
