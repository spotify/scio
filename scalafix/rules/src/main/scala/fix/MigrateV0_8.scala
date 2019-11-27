package fix
package v0_8_0

import scalafix.v1._
import scala.meta._

final class FixRunWithContext extends SemanticRule("FixRunWithContext") {

  private def fixSubtree(name: String)(doc: SyntacticDocument): Patch =
    doc.tree.collect {
      case t @ Term.Select(Term.Name(`name`), Term.Name("isCompleted")) =>
        Patch.empty
      case t @ Term.Select(Term.Name(`name`), Term.Name("state")) =>
        Patch.empty
      case t @ Term.Select(Term.Name(`name`), x) =>
        Patch.replaceTree(t, s"$name.waitUntilFinish.$x")
      case t @ Term.Apply(tn, ps) if ps.exists(x => x.toString == name) =>
        val ps2 =
          ps.map {
            case t if t.toString == name => q"$t.waitUntilFinish()"
            case t                       => t
          }
        Patch.replaceTree(t, Term.Apply(tn, ps2).toString)
    }.asPatch

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      // Convert val`x` of type ScioExecutionContext to ScioResult
      // when `x` is passed to a method oo
      // if a method is called on `x` (except `isCompleted and `state`)
      case t @ q"val ${x} = runWithContext(${body})" =>
        val children = t.parent.toList.flatMap(_.children).filterNot(_ == t)
        val name = x.toString()
        children.map { c =>
          fixSubtree(name)(SyntacticDocument.fromTree(c))
        }.asPatch
      // Convert ScioExecutionContext to ScioResult in methods that return a ScioResult
      case t @ q"def $fn(..$ps): ScioResult = {$body}" =>
        Patch.addRight(t, ".waitUntilFinish()")
      case t =>
        Patch.empty
    }.asPatch
}

final class FixScioIO extends SemanticRule("FixScioIO") {

  // Check that the method is a member of an implementation of ScioIO
  private def isScioIOMember(t: Tree)(implicit doc: SemanticDocument) =
    t.parent
      .collect {
        case p @ Template(_) =>
          p.inits.exists(_.tpe.symbol == Symbol("com/spotify/scio/io/ScioIO#"))
      }
      .getOrElse(false)

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      // fix return type (No Future 🤘)
      case t @ q"""..$mods def write(
                      $sName: SCollection[$sTpe],
                      $psName: $psTpe): $ret = $impl""" if isScioIOMember(t) =>
        ret.collect {
          case r @ Type.Apply(Type.Name("Future"), List(tapTpe)) =>
            Patch.replaceTree(r, tapTpe.syntax)
        }.asPatch
    }.asPatch
}

final class FixSyntaxImports extends SemanticRule("FixSyntaxImports") {
  private val imports =
    scala.collection.mutable.ArrayBuffer.empty[(String, String)]

  // Check that the package is not imported multiple times in the same file
  private def addImport(p: Position, i: Importer): Patch = {
    val Importer(s) = i
    val Input.VirtualFile(path, _) = p.input

    val t = (s.toString, path)
    if (!imports.contains(t)) {
      imports += t
      Patch.addGlobalImport(i)
    } else Patch.empty
  }

  object JDBC {
    val fns =
      List("JdbcScioContext", "JdbcSCollection")

    val `import` = importer"com.spotify.scio.jdbc._"
  }

  object BQ {
    val fns =
      List("toBigQueryScioContext", "toBigQuerySCollection")

    val `import` = importer"com.spotify.scio.bigquery._"
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val renameSymbols =
      Patch.replaceSymbols(
        "com/spotify/scio/transforms/AsyncLookupDoFn." ->
          "com/spotify/scio/transforms/BaseAsyncLookupDoFn.",
        "com.spotify.scio.jdbc.JdbcScioContext." ->
          "com.spotify.scio.jdbc.syntax.JdbcScioContextOps."
      )

    doc.tree.collect {
      case i @ Importee.Name(Name.Indeterminate(n)) if JDBC.fns.contains(n) =>
        Patch.removeImportee(i) + addImport(i.pos, JDBC.`import`)
      case i @ Importee.Name(Name.Indeterminate(n)) if BQ.fns.contains(n) =>
        Patch.removeImportee(i) + addImport(i.pos, BQ.`import`)
    }.asPatch + renameSymbols
  }
}

final class FixContextClose extends SemanticRule("FixContextClose") {
  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ q"$x.close()" =>
        x.symbol.info.get.signature match {
          case ValueSignature(TypeRef(_, tpe, _))
              if tpe == Symbol("com/spotify/scio/ScioContext#") =>
            Patch.replaceTree(t, q"$x.run()".syntax)
          case _ =>
            Patch.empty
        }
    }.asPatch
}

final class FixTensorflow extends SemanticRule("FixTensorflow") {
  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ Term.Select(s, Term.Name("saveAsTfExampleFile")) =>
        Patch.replaceTree(t, q"$s.saveAsTfRecordFile".syntax)
    }.asPatch
}

final class FixBigQueryDeprecations extends SemanticRule("FixBigQueryDeprecations") {
  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(s, Term.Name("bigQueryTable")), x :: xs) =>
        val term = x.symbol.info match {
          case None => q"Table.Spec($x)"
          case _    => q"Table.Ref($x)"
        }
        val syntax = Term.Apply(Term.Select(s, Term.Name("bigQueryTable")), term :: xs).syntax
        Patch.replaceTree(t, syntax)
    }.asPatch
}

final class ConsistenceJoinNames extends SemanticRule("ConsistenceJoinNames") {
  override def fix(implicit doc: _root_.scalafix.v1.SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(fun, args) =>
        fun match {
          case t2 @ Term.Select(qual, name) =>
            name match {
              case t3 @ Term.Name("hashLeftJoin") =>
                Patch.replaceTree(t3, "hashLeftOuterJoin") + renameNamedArgs(args)
              case t3 @ Term.Name("skewedLeftJoin") =>
                Patch.replaceTree(t3, "skewedLeftOuterJoin") + renameNamedArgs(args)
              case t3 @ Term.Name("sparseOuterJoin") =>
                Patch.replaceTree(t3, "sparseFullOuterJoin") + renameNamedArgs(args)
              case _ => Patch.empty
            }
        }
    }
  }.asPatch

  private def renameNamedArgs(args: List[Term]): Patch =
    args.collect {
      case Term.Assign(lhs, rhs) =>
        lhs match {
          case t2 @ Term.Name("that") => Patch.replaceTree(t2, "right")
          case _                      => Patch.empty
        }
      case _ => Patch.empty
    }.asPatch
}
