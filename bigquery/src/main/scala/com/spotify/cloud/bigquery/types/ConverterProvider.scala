package com.spotify.cloud.bigquery.types

import com.google.api.services.bigquery.model.TableRow
import com.spotify.cloud.bigquery.types.MacroUtil._
import org.joda.time.Instant

import scala.language.experimental.macros
import scala.reflect.macros._

private[types] object ConverterProvider {

  def fromTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[(TableRow => T)] = {
    import c.universe._
    val tpe = implicitly[c.WeakTypeTag[T]].tpe
    val r = fromTableRowInternal(c)(tpe)
    debug(s"ConverterProvider.fromTableRowImpl[${weakTypeOf[T]}]:")
    debug(r)
    c.Expr[(TableRow => T)](r)
  }

  def toTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[(T => TableRow)] = {
    import c.universe._
    val tpe = implicitly[c.WeakTypeTag[T]].tpe
    val r = toTableRowInternal(c)(tpe)
    debug(s"ConverterProvider.toTableRowImpl[${weakTypeOf[T]}]:")
    debug(r)
    c.Expr[(T => TableRow)](r)
  }

  private def fromTableRowInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Case class helpers
    // =======================================================================
    // TODO: deduplicate

    def isCaseClass(t: Type): Boolean = !t.toString.startsWith("scala.") &&
      List(typeOf[Product], typeOf[Serializable], typeOf[Equals]).forall(b => t.baseClasses.contains(b.typeSymbol))
    def isField(s: Symbol): Boolean = !s.isSynthetic && s.isTerm && s.isPrivate
    def getFields(t: Type): Iterable[Symbol] = t.decls.filter(isField)

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(tree: Tree, tpe: Type): Tree = {
      val s = q"$tree.toString"
      tpe match {
        case t if t =:= typeOf[Int] => q"$s.toInt"
        case t if t =:= typeOf[Long] => q"$s.toLong"
        case t if t =:= typeOf[Float] => q"$s.toFloat"
        case t if t =:= typeOf[Double] => q"$s.toDouble"
        case t if t =:= typeOf[Boolean] => q"$s.toBoolean"
        case t if t =:= typeOf[String] => q"$s"
        case t if t =:= typeOf[Instant] => q"_root_.org.joda.time.Instant.parse($s)"
        case t if isCaseClass(t) =>
          val fn = TermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree.asInstanceOf[${p(c, GBQM)}.TableRow]
                ${constructor(t, fn)}
              }
          """
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
    }

    def option(tree: Tree, tpe: Type): Tree = q"if ($tree == null) None else Some(${cast(tree, tpe)})"

    def list(tree: Tree, tpe: Type): Tree = {
      val jl = tq"_root_.java.util.List[AnyRef]"
      q"$tree.asInstanceOf[$jl].asScala.map(x => ${cast(q"x", tpe)}).toList"
    }

    def field(symbol: Symbol, fn: TermName): Tree = {
      // TODO: figure out why there's trailing spaces
      val name = symbol.name.toString.trim
      val tpe = symbol.typeSignature
      val TypeRef(_, _, args) = tpe

      val tree = q"$fn.get($name)"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        option(tree, args.head)
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        list(tree, args.head)
      } else {
        cast(tree, tpe)
      }
    }

    def constructor(tpe: Type, fn: TermName): Tree = {
      val companion = tpe.typeSymbol.companion
      val gets = tpe.erasure match {
        case t if isCaseClass(t) => getFields(t).map(s => field(s, fn))
        case t => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      q"$companion(..$gets)"
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    q"""(r: ${p(c, GBQM)}.TableRow) => {
          import _root_.scala.collection.JavaConverters._
          ${constructor(tpe, TermName("r"))}
        }
    """
  }

  private def toTableRowInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Case class helpers
    // =======================================================================
    // TODO: deduplicate

    def isCaseClass(t: Type): Boolean = !t.toString.startsWith("scala.") &&
      List(typeOf[Product], typeOf[Serializable], typeOf[Equals]).forall(b => t.baseClasses.contains(b.typeSymbol))
    def isField(s: Symbol): Boolean = !s.isSynthetic && s.isTerm && s.isPrivate
    def getFields(t: Type): Iterable[Symbol] = t.decls.filter(isField)

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(tree: Tree, tpe: Type): Tree = {
      tpe match {
        case t if t =:= typeOf[Int] => tree
        case t if t =:= typeOf[Long] => tree
        case t if t =:= typeOf[Float] => tree
        case t if t =:= typeOf[Double] => tree
        case t if t =:= typeOf[Boolean] => tree
        case t if t =:= typeOf[String] => tree
        case t if t =:= typeOf[Instant] => tree
        case t if isCaseClass(t) =>
          val fn = TermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree
                ${constructor(t, fn)}
              }
          """
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
    }

    def option(tree: Tree, tpe: Type): Tree = q"if ($tree.isDefined) ${cast(q"$tree.get", tpe)} else null"

    def list(tree: Tree, tpe: Type): Tree = q"$tree.map(x => ${cast(q"x", tpe)}).asJava"

    def field(symbol: Symbol, fn: TermName): (String, Tree) = {
      // TODO: figure out why there's trailing spaces
      val name = symbol.name.toString.trim
      val tpe = symbol.typeSignature
      val TypeRef(_, _, args) = tpe

      val tree = q"$fn.${TermName(name)}"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        (name, option(tree, args.head))
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        (name, list(tree, args.head))
      } else {
        (name, cast(tree, tpe))
      }
    }

    def constructor(tpe: Type, fn: TermName): Tree = {
      val sets = tpe.erasure match {
        case t if isCaseClass(t) => getFields(t).map(s => field(s, fn))
        case t => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      val tr = q"new ${p(c, GBQM)}.TableRow()"
      sets.foldLeft(tr) { case (acc, (name, value)) =>
        q"$acc.set($name, $value)"
      }
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    q"""(r: $tpe) => {
          import _root_.scala.collection.JavaConverters._
          ${constructor(tpe, TermName("r"))}
        }
    """
  }

}
