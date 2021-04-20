/*
 * Copyright 2019 Spotify AB.
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
import com.google.protobuf.ByteString
import com.spotify.scio.bigquery.types.MacroUtil._
import com.spotify.scio.bigquery.validation.{OverrideTypeProvider, OverrideTypeProviderFinder}
import org.apache.avro.generic.GenericRecord
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import scala.reflect.macros._

private[types] object ConverterProvider {
  def fromAvroImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[GenericRecord => T] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val r = fromAvroInternal(c)(tpe)
    debug(s"ConverterProvider.fromAvroImpl[$tpe]:")
    debug(r)
    c.Expr[GenericRecord => T](r)
  }

  def toAvroImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[T => GenericRecord] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val r = toAvroInternal(c)(tpe)
    debug(s"ConverterProvider.toAvroInternal[$tpe]:")
    debug(r)
    c.Expr[T => GenericRecord](r)
  }

  def fromTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[TableRow => T] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val r = fromTableRowInternal(c)(tpe)
    debug(s"ConverterProvider.fromTableRowImpl[$tpe]:")
    debug(r)
    c.Expr[TableRow => T](r)
  }

  def toTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[T => TableRow] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val r = toTableRowInternal(c)(tpe)
    debug(s"ConverterProvider.toTableRowImpl[$tpe]:")
    debug(r)
    c.Expr[T => TableRow](r)
  }

  // =======================================================================

  private def fromAvroInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(tree: Tree, tpe: Type): Tree = {
      val provider: OverrideTypeProvider =
        OverrideTypeProviderFinder.getProvider
      tpe match {
        case t if provider.shouldOverrideType(c)(t) =>
          provider.createInstance(c)(t, q"$tree")
        case t if t =:= typeOf[Boolean] => q"$tree.asInstanceOf[Boolean]"
        case t if t =:= typeOf[Int]     => q"$tree.asInstanceOf[Long].toInt"
        case t if t =:= typeOf[Long]    => q"$tree.asInstanceOf[Long]"
        case t if t =:= typeOf[Float]   => q"$tree.asInstanceOf[Double].toFloat"
        case t if t =:= typeOf[Double]  => q"$tree.asInstanceOf[Double]"
        case t if t =:= typeOf[String]  => q"$tree.toString"
        case t if t =:= typeOf[BigDecimal] =>
          q"_root_.com.spotify.scio.bigquery.Numeric.parse($tree)"

        case t if t =:= typeOf[ByteString] =>
          val b = q"$tree.asInstanceOf[_root_.java.nio.ByteBuffer]"
          q"_root_.com.google.protobuf.ByteString.copyFrom($b.asReadOnlyBuffer())"
        case t if t =:= typeOf[Array[Byte]] =>
          val b = q"$tree.asInstanceOf[_root_.java.nio.ByteBuffer]"
          q"_root_.java.util.Arrays.copyOfRange($b.array(), $b.position(), $b.limit())"

        case t if t =:= typeOf[Instant] =>
          q"_root_.com.spotify.scio.bigquery.Timestamp.parse($tree)"
        case t if t =:= typeOf[LocalDate] =>
          q"_root_.com.spotify.scio.bigquery.Date.parse($tree)"
        case t if t =:= typeOf[LocalTime] =>
          q"_root_.com.spotify.scio.bigquery.Time.parse($tree)"
        case t if t =:= typeOf[LocalDateTime] =>
          q"_root_.com.spotify.scio.bigquery.DateTime.parse($tree.toString)"
        case t if t =:= typeOf[Geography] =>
          q"_root_.com.spotify.scio.bigquery.types.Geography($tree.toString)"

        case t if isCaseClass(c)(t) =>
          val fn = TermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree.asInstanceOf[_root_.org.apache.avro.generic.GenericRecord]
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
      q"asScala($tree.asInstanceOf[$jl]).iterator.map(x => ${cast(q"x", tpe)}).toList"
    }

    def field(symbol: Symbol, fn: TermName): Tree = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$fn.get($name)"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        option(tree, tpe.typeArgs.head)
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        list(tree, tpe.typeArgs.head)
      } else {
        cast(tree, tpe)
      }
    }

    def constructor(tpe: Type, fn: TermName): Tree = {
      val companion = tpe.typeSymbol.companion
      val gets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      q"$companion(..$gets)"
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    val tn = TermName("r")
    q"""(r: _root_.org.apache.avro.generic.GenericRecord) => {
          import _root_.scala.collection.compat.extra.CollectionConverters._
          ${constructor(tpe, tn)}
        }
    """
  }

  private def toAvroInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(tree: Tree, tpe: Type): Tree = {
      val provider: OverrideTypeProvider =
        OverrideTypeProviderFinder.getProvider
      tpe match {
        case t if provider.shouldOverrideType(c)(t) => q"$tree.toString"
        case t if t =:= typeOf[Boolean]             => tree
        case t if t =:= typeOf[Int]                 => q"$tree.toLong"
        case t if t =:= typeOf[Long]                => tree
        case t if t =:= typeOf[Float]               => q"$tree.toDouble"
        case t if t =:= typeOf[Double]              => tree
        case t if t =:= typeOf[String]              => tree

        case t if t =:= typeOf[BigDecimal] =>
          q"_root_.com.spotify.scio.bigquery.Numeric($tree).toString"
        case t if t =:= typeOf[ByteString]  => q"_root_.java.nio.ByteBuffer.wrap($tree.toByteArray)"
        case t if t =:= typeOf[Array[Byte]] => q"_root_.java.nio.ByteBuffer.wrap($tree)"

        case t if t =:= typeOf[Instant] => q"$tree.getMillis * 1000"
        case t if t =:= typeOf[LocalDate] =>
          q"_root_.com.spotify.scio.bigquery.Date($tree)"
        case t if t =:= typeOf[LocalTime] =>
          q"_root_.com.spotify.scio.bigquery.Time($tree)"
        case t if t =:= typeOf[LocalDateTime] =>
          q"_root_.com.spotify.scio.bigquery.DateTime($tree)"

        case t if t =:= typeOf[Geography] =>
          // different than nested record match below, even though this is a case class
          q"$tree.wkt"

        case t if isCaseClass(c)(t) => // nested records
          val fn = TermName("r" + t.typeSymbol.name)
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

    def list(tree: Tree, tpe: Type): Tree =
      q"asJava($tree.map(x => ${cast(q"x", tpe)}))"

    def field(symbol: Symbol, fn: TermName): (String, Tree) = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$fn.${TermName(name)}"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        (name, option(tree, tpe.typeArgs.head))
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        (name, list(tree, tpe.typeArgs.head))
      } else {
        (name, cast(tree, tpe))
      }
    }

    def constructor(tpe: Type, fn: TermName): Tree = {
      val sets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }

      val header =
        q"val result = new _root_.org.apache.avro.generic.GenericRecordBuilder(${p(c, SType)}.avroSchemaOf[$tpe])"
      val body = sets.map { case (name, value) =>
        q"result.set($name, $value)"
      }
      val footer = q"result.build()"
      q"{$header; ..$body; $footer}"
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    val tn = TermName("r")
    q"""(r: $tpe) => {
          import _root_.scala.collection.compat.extra.CollectionConverters._
          ${constructor(tpe, tn)}
        }
    """
  }

  // =======================================================================

  private def fromTableRowInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Converter helpers
    // =======================================================================
    def cast(tree: Tree, tpe: Type): Tree = {
      val provider: OverrideTypeProvider =
        OverrideTypeProviderFinder.getProvider
      val s = q"$tree.toString"
      tpe match {
        case t if provider.shouldOverrideType(c)(t) =>
          provider.createInstance(c)(t, q"$tree")
        case t if t =:= typeOf[Boolean] => q"$s.toBoolean"
        case t if t =:= typeOf[Int]     => q"$s.toInt"
        case t if t =:= typeOf[Long]    => q"$s.toLong"
        case t if t =:= typeOf[Float]   => q"$s.toFloat"
        case t if t =:= typeOf[Double]  => q"$s.toDouble"
        case t if t =:= typeOf[String]  => q"$s"
        case t if t =:= typeOf[BigDecimal] =>
          q"_root_.com.spotify.scio.bigquery.Numeric($s)"

        case t if t =:= typeOf[ByteString] =>
          val b =
            q"_root_.com.google.common.io.BaseEncoding.base64().decode($s)"
          q"_root_.com.google.protobuf.ByteString.copyFrom($b)"
        case t if t =:= typeOf[Array[Byte]] =>
          q"_root_.com.google.common.io.BaseEncoding.base64().decode($s)"

        case t if t =:= typeOf[Instant] =>
          q"_root_.com.spotify.scio.bigquery.Timestamp.parse($s)"
        case t if t =:= typeOf[LocalDate] =>
          q"_root_.com.spotify.scio.bigquery.Date.parse($s)"
        case t if t =:= typeOf[LocalTime] =>
          q"_root_.com.spotify.scio.bigquery.Time.parse($s)"
        case t if t =:= typeOf[LocalDateTime] =>
          q"_root_.com.spotify.scio.bigquery.DateTime.parse($s)"

        case t if t =:= typeOf[Geography] =>
          // different than nested record match below, even though this is a case class
          q"_root_.com.spotify.scio.bigquery.types.Geography($s)"

        case t if isCaseClass(c)(t) => // nested records
          val fn = TermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree.asInstanceOf[_root_.java.util.Map[String, AnyRef]]
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
      q"asScala($tree.asInstanceOf[$jl].iterator).map(x => ${cast(q"x", tpe)}).toList"
    }

    def field(symbol: Symbol, fn: TermName): Tree = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$fn.get($name)"
      def nonNullTree(fType: String) =
        q"""{
          val v = $fn.get($name)
          if (v == null) {
            throw new NullPointerException($fType + " field \"" + $name + "\" is null")
          }
          v
        }"""
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        option(tree, tpe.typeArgs.head)
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        list(nonNullTree("REPEATED"), tpe.typeArgs.head)
      } else {
        cast(nonNullTree("REQUIRED"), tpe)
      }
    }

    def constructor(tpe: Type, fn: TermName): Tree = {
      val companion = tpe.typeSymbol.companion
      val gets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      q"$companion(..$gets)"
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    val tn = TermName("r")
    q"""(r: _root_.java.util.Map[String, AnyRef]) => {
          import _root_.scala.collection.compat.extra.CollectionConverters._
          ${constructor(tpe, tn)}
        }
    """
  }

  // =======================================================================

  private def toTableRowInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(tree: Tree, tpe: Type): Tree = {
      val provider: OverrideTypeProvider =
        OverrideTypeProviderFinder.getProvider
      tpe match {
        case t if provider.shouldOverrideType(c)(t) => q"$tree.toString"
        case t if t =:= typeOf[Boolean]             => tree
        case t if t =:= typeOf[Int]                 => tree
        case t if t =:= typeOf[Long]                => tree
        case t if t =:= typeOf[Float]               => tree
        case t if t =:= typeOf[Double]              => tree
        case t if t =:= typeOf[String]              => tree

        case t if t =:= typeOf[BigDecimal] =>
          q"_root_.com.spotify.scio.bigquery.Numeric($tree).toString"
        case t if t =:= typeOf[ByteString] =>
          q"_root_.com.google.common.io.BaseEncoding.base64().encode($tree.toByteArray)"
        case t if t =:= typeOf[Array[Byte]] =>
          q"_root_.com.google.common.io.BaseEncoding.base64().encode($tree)"

        case t if t =:= typeOf[Instant] =>
          q"_root_.com.spotify.scio.bigquery.Timestamp($tree)"
        case t if t =:= typeOf[LocalDate] =>
          q"_root_.com.spotify.scio.bigquery.Date($tree)"
        case t if t =:= typeOf[LocalTime] =>
          q"_root_.com.spotify.scio.bigquery.Time($tree)"
        case t if t =:= typeOf[LocalDateTime] =>
          q"_root_.com.spotify.scio.bigquery.DateTime($tree)"

        case t if t =:= typeOf[Geography] =>
          // different than nested record match below, even though this is a case class
          q"$tree.wkt"

        case t if isCaseClass(c)(t) => // nested records
          val fn = TermName("r" + t.typeSymbol.name)
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

    def list(tree: Tree, tpe: Type): Tree =
      q"asJava($tree.map(x => ${cast(q"x", tpe)}))"

    def field(symbol: Symbol, fn: TermName): (String, Tree) = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$fn.${TermName(name)}"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        (name, option(tree, tpe.typeArgs.head))
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        (name, list(tree, tpe.typeArgs.head))
      } else {
        (name, cast(tree, tpe))
      }
    }

    def constructor(tpe: Type, fn: TermName): Tree = {
      val sets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      val header = q"val result = new ${p(c, GModel)}.TableRow()"
      val body = sets.map { case (name, value) =>
        q"if (${p(c, SBQ)}.types.ConverterUtil.notNull($value)) result.set($name, $value)"
      }
      val footer = q"result"
      q"{$header; ..$body; $footer}"
    }

    // =======================================================================
    // Entry point
    // =======================================================================

    val tn = TermName("r")
    q"""(r: $tpe) => {
          import _root_.scala.collection.compat.extra.CollectionConverters._
          ${constructor(tpe, tn)}
        }
    """
  }
}

object ConverterUtil {
  @inline final def notNull[@specialized(Boolean, Int, Long, Float, Double) T](x: T): Boolean =
    x != null
}
