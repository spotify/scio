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
    debug(c)(s"ConverterProvider.fromAvroImpl[$tpe]:", r)
    c.Expr[GenericRecord => T](r)
  }

  def toAvroImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[T => GenericRecord] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val r = toAvroInternal(c)(tpe)
    debug(c)(s"ConverterProvider.toAvroInternal[$tpe]:", r)
    c.Expr[T => GenericRecord](r)
  }

  def fromTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[TableRow => T] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val r = fromTableRowInternal(c)(tpe)
    debug(c)(s"ConverterProvider.fromTableRowImpl[$tpe]:", r)
    c.Expr[TableRow => T](r)
  }

  def toTableRowImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[T => TableRow] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val r = toTableRowInternal(c)(tpe)
    debug(c)(s"ConverterProvider.toTableRowImpl[$tpe]:", r)
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
        case t if t =:= typeOf[Boolean] =>
          q"$tree.asInstanceOf[Boolean]"
        case t if t =:= typeOf[Int] =>
          q"$tree.asInstanceOf[Long].toInt"
        case t if t =:= typeOf[Long] =>
          q"$tree.asInstanceOf[Long]"
        case t if t =:= typeOf[Float] =>
          q"$tree.asInstanceOf[Double].toFloat"
        case t if t =:= typeOf[Double] =>
          q"$tree.asInstanceOf[Double]"
        case t if t =:= typeOf[String] =>
          q"$tree.toString"
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
        case t if t =:= typeOf[Json] =>
          q"_root_.com.spotify.scio.bigquery.types.Json($tree.toString)"
        case t if t =:= typeOf[BigNumeric] =>
          q"_root_.com.spotify.scio.bigquery.types.BigNumeric.parse($tree)"
        case t if isCaseClass(c)(t) =>
          val nestedRecord = TermName("r" + t.typeSymbol.name)
          q"""{
                val $nestedRecord = $tree.asInstanceOf[_root_.org.apache.avro.generic.GenericRecord]
                ${constructor(t, Ident(nestedRecord))}
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

    def field(symbol: Symbol, record: Tree): Tree = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$record.get($name)"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        option(tree, tpe.typeArgs.head)
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        list(tree, tpe.typeArgs.head)
      } else {
        cast(tree, tpe)
      }
    }

    def constructor(tpe: Type, record: Tree): Tree = {
      val companion = tpe.typeSymbol.companion
      val gets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, record))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      q"$companion(..$gets)"
    }

    // =======================================================================
    // Entry point
    // =======================================================================
    q"""(r: _root_.org.apache.avro.generic.GenericRecord) => {
          import _root_.scala.jdk.javaapi.CollectionConverters._
          ${constructor(tpe, q"r")}
        }
    """
  }

  private def toAvroInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(fieldName: String, tree: Tree, tpe: Type): Tree = {
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
          q"_root_.com.spotify.scio.bigquery.Numeric.bytes($tree)"
        case t if t =:= typeOf[ByteString] =>
          q"_root_.java.nio.ByteBuffer.wrap($tree.toByteArray)"
        case t if t =:= typeOf[Array[Byte]] =>
          q"_root_.java.nio.ByteBuffer.wrap($tree)"

        case t if t =:= typeOf[Instant] =>
          q"_root_.com.spotify.scio.bigquery.Timestamp.micros($tree)"
        case t if t =:= typeOf[LocalDate] =>
          q"_root_.com.spotify.scio.bigquery.Date.days($tree)"
        case t if t =:= typeOf[LocalTime] =>
          q"_root_.com.spotify.scio.bigquery.Time.micros($tree)"
        case t if t =:= typeOf[LocalDateTime] =>
          // LocalDateTime is read as avro string
          // on write we should use `local-timestamp-micros`
          q"_root_.com.spotify.scio.bigquery.DateTime.format($tree)"

        // different than nested record match below, even though thore are case classes
        case t if t =:= typeOf[Geography] =>
          q"$tree.wkt"
        case t if t =:= typeOf[Json] =>
          q"$tree.wkt"
        case t if t =:= typeOf[BigNumeric] =>
          q"_root_.com.spotify.scio.bigquery.types.BigNumeric.bytes($tree)"

        // nested records
        case t if isCaseClass(c)(t) =>
          val fn = TermName("r" + fieldName)
          q"""{
                val $fn = $tree
                ${constructor(fieldName, t, fn)}
              }
          """
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
    }

    def option(fieldName: String, tree: Tree, tpe: Type): Tree =
      q"if ($tree.isDefined) ${cast(fieldName, q"$tree.get", tpe)} else null"

    def list(fieldName: String, tree: Tree, tpe: Type): Tree =
      q"asJava($tree.map(x => ${cast(fieldName, q"x", tpe)}))"

    def field(symbol: Symbol, fn: TermName): (String, Tree) = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$fn.${TermName(name)}"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        (name, option(name, tree, tpe.typeArgs.head))
      } else if (tpe.erasure =:= typeOf[List[_]].erasure) {
        (name, list(name, tree, tpe.typeArgs.head))
      } else {
        (name, cast(name, tree, tpe))
      }
    }

    def constructor(fieldName: String, tpe: Type, fn: TermName): Tree = {
      val sets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }

      val header = {
        q"""
            // Schema name must match fieldName, rather than nested case class name
            val result = {
              import _root_.scala.jdk.CollectionConverters._
              val recordSchema = ${p(c, SType)}.avroSchemaOf[$tpe]
              new _root_.org.apache.avro.generic.GenericRecordBuilder(
                _root_.org.apache.avro.Schema.createRecord(
                  $fieldName,
                  recordSchema.getDoc,
                  recordSchema.getNamespace,
                  recordSchema.isError,
                  recordSchema.getFields.asScala.map(f => new _root_.org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())).asJava
                )
              )
            }
        """
      }
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
          import _root_.scala.jdk.javaapi.CollectionConverters._
          ${constructor(tpe.typeSymbol.name.toString, tpe, tn)}
        }
    """
  }

  // =======================================================================

  private def fromTableRowInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    val ops = q"_root_.com.spotify.scio.bigquery.syntax.TableRowOps"
    val bs = q"_root_.com.google.protobuf.ByteString"

    // =======================================================================
    // Converter helpers
    // =======================================================================
    def cast(tree: Tree, tpe: Type): Tree = {
      val provider: OverrideTypeProvider = OverrideTypeProviderFinder.getProvider
      tpe match {
        case t if provider.shouldOverrideType(c)(t) => provider.createInstance(c)(t, q"$tree")
        case t if t =:= typeOf[Boolean]             => q"$ops.boolean($tree)"
        case t if t =:= typeOf[Int]                 => q"$ops.int($tree)"
        case t if t =:= typeOf[Long]                => q"$ops.long($tree)"
        case t if t =:= typeOf[Float]               => q"$ops.float($tree)"
        case t if t =:= typeOf[Double]              => q"$ops.double($tree)"
        case t if t =:= typeOf[String]              => q"$ops.string($tree)"
        case t if t =:= typeOf[BigDecimal]          => q"$ops.numeric($tree)"
        case t if t =:= typeOf[Array[Byte]]         => q"$ops.bytes($tree)"
        case t if t =:= typeOf[ByteString]          => q"$bs.copyFrom($ops.bytes($tree))"
        case t if t =:= typeOf[Instant]             => q"$ops.timestamp($tree)"
        case t if t =:= typeOf[LocalDate]           => q"$ops.date($tree)"
        case t if t =:= typeOf[LocalTime]           => q"$ops.time($tree)"
        case t if t =:= typeOf[LocalDateTime]       => q"$ops.datetime($tree)"
        case t if t =:= typeOf[Geography]           => q"$ops.geography($tree)"
        case t if t =:= typeOf[Json]                => q"$ops.json($tree)"
        case t if t =:= typeOf[BigNumeric]          => q"$ops.bignumeric($tree)"
        case t if isCaseClass(c)(t)                 =>
          // nested records
          val r = TermName("r" + t.typeSymbol.name)
          q"""{
            val $r = $ops.record($tree)
            ${constructor(t, r)}
          }"""
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
    }

    def field(symbol: Symbol, row: TermName): Tree = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      tpe match {
        case t if t.erasure =:= typeOf[Option[_]].erasure =>
          q"$ops.nullable($name)($row).map(x => ${cast(q"x", t.typeArgs.head)})"
        case t if t.erasure =:= typeOf[List[_]].erasure =>
          q"$ops.repeated($name)($row).map(x => ${cast(q"x", t.typeArgs.head)})"
        case t =>
          q"${cast(q"$ops.required($name)($row)", t)}"
      }
    }

    def constructor(tpe: Type, row: TermName): Tree = {
      val companion = tpe.typeSymbol.companion
      val gets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, row))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      q"$companion(..$gets)"
    }

    // =======================================================================
    // Entry point
    // =======================================================================
    val r = TermName("r")
    q"($r: ${typeOf[TableRow]}) => ${constructor(tpe, r)}"
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
        case t if t =:= typeOf[Long]   => q"$tree.toString" // json doesn't support long
        case t if t =:= typeOf[Float]  => q"$tree.toDouble" // json doesn't support float
        case t if t =:= typeOf[Double] => tree
        case t if t =:= typeOf[String] => tree

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

        // different than nested record match below, even though those are case classes
        case t if t =:= typeOf[Geography] =>
          q"$tree.wkt"
        case t if t =:= typeOf[Json] =>
          q"$tree.wkt"
        case t if t =:= typeOf[BigNumeric] =>
          // for TableRow/json, use string to avoid precision loss (like numeric)
          q"$tree.wkt.toString"

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
          import _root_.scala.jdk.javaapi.CollectionConverters._
          ${constructor(tpe, tn)}
        }
    """
  }
}

object ConverterUtil {
  @inline final def notNull[@specialized(Boolean, Int, Long, Float, Double) T](x: T): Boolean =
    x != null
}
