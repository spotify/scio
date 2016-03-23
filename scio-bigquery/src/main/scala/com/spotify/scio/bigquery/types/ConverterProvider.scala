/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bigquery.types

import com.google.api.services.bigquery.model.TableRow
import com.spotify.scio.bigquery.types.MacroUtil._
import org.joda.time.Instant

import scala.language.experimental.macros
import scala.reflect.macros._

private[types] object ConverterProvider {

  // TODO: scala 2.11
  // def fromTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[(TableRow => T)] = {
  def fromTableRowImpl[T: c.WeakTypeTag](c: Context): c.Expr[(TableRow => T)] = {
    import c.universe._
    val tpe = implicitly[c.WeakTypeTag[T]].tpe
    val r = fromTableRowInternal(c)(tpe)
    debug(s"ConverterProvider.fromTableRowImpl[${weakTypeOf[T]}]:")
    debug(r)
    c.Expr[(TableRow => T)](r)
  }

  // TODO: scala 2.11
  // def toTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[(T => TableRow)] = {
  def toTableRowImpl[T: c.WeakTypeTag](c: Context): c.Expr[(T => TableRow)] = {
    import c.universe._
    val tpe = implicitly[c.WeakTypeTag[T]].tpe
    val r = toTableRowInternal(c)(tpe)
    debug(s"ConverterProvider.toTableRowImpl[${weakTypeOf[T]}]:")
    debug(r)
    c.Expr[(T => TableRow)](r)
  }

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  // TODO: scala 2.11
  // private def fromTableRowInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
  private def fromTableRowInternal(c: Context)(tpe: c.Type): c.Tree = {
    import c.universe._

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
        case t if t =:= typeOf[Instant] => q"_root_.com.spotify.scio.bigquery.Timestamp.parse($s)"
        case t if isCaseClass(c)(t) =>
          // TODO: scala 2.11
          // val fn = TermName("r" + t.typeSymbol.name)
          val fn = newTermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree.asInstanceOf[java.util.Map[String, AnyRef]]
                ${constructor(t, fn)}
              }
          """
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
    }

    def option(tree: Tree, tpe: Type): Tree =
      q"if ($tree == null) None else Some(${cast(tree, tpe)})"

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
      // TODO: scala 2.11
      // val companion = tpe.typeSymbol.companion
      val companion = tpe.typeSymbol.companionSymbol
      val gets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case t => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      q"$companion(..$gets)"
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    // TODO: scala 2.11
    // val tn = TermName("r")
    val tn = newTermName("r")
    q"""(r: java.util.Map[String, AnyRef]) => {
          import _root_.scala.collection.JavaConverters._
          ${constructor(tpe, tn)}
        }
    """
  }
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  // TODO: scala 2.11
  // private def toTableRowInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
  private def toTableRowInternal(c: Context)(tpe: c.Type): c.Tree = {
    import c.universe._

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
        case t if t =:= typeOf[Instant] => q"_root_.com.spotify.scio.bigquery.Timestamp($tree)"
        case t if isCaseClass(c)(t) =>
          // TODO: scala 2.11
          // val fn = TermName("r" + t.typeSymbol.name)
          val fn = newTermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree
                ${constructor(t, fn)}
              }
          """
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
    }

    def option(tree: Tree, tpe: Type): Tree =
      q"if ($tree.isDefined) ${cast(q"$tree.get", tpe)} else null"

    def list(tree: Tree, tpe: Type): Tree = q"$tree.map(x => ${cast(q"x", tpe)}).asJava"

    def field(symbol: Symbol, fn: TermName): (String, Tree) = {
      // TODO: figure out why there's trailing spaces
      val name = symbol.name.toString.trim
      val tpe = symbol.typeSignature
      val TypeRef(_, _, args) = tpe

      // TODO: scala 2.11
      // val tree = q"$fn.${TermName(name)}"
      val tree = q"$fn.${newTermName(name)}"
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
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case t => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      val tr = q"new ${p(c, GModel)}.TableRow()"
      sets.foldLeft(tr) { case (acc, (name, value)) =>
        q"$acc.set($name, $value)"
      }
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    // TODO: scala 2.11
    // val tn = TermName("r")
    val tn = newTermName("r")
    q"""(r: $tpe) => {
          import _root_.scala.collection.JavaConverters._
          ${constructor(tpe, tn)}
        }
    """
  }
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length

}
