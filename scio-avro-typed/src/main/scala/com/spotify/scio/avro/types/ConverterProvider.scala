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

package com.spotify.scio.avro.types

import com.google.protobuf.ByteString
import com.spotify.scio.avro.types.MacroUtil._
import org.apache.avro.generic.GenericRecord

import scala.reflect.macros._

private[types] object ConverterProvider {
  def fromGenericRecordImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[GenericRecord => T] = {
    val tpe = implicitly[c.WeakTypeTag[T]].tpe
    val r = fromGenericRecordInternal(c)(tpe)

    c.Expr[GenericRecord => T](r)
  }

  def toGenericRecordImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[T => GenericRecord] = {
    val tpe = implicitly[c.WeakTypeTag[T]].tpe
    val r = toGenericRecordInternal(c)(tpe)

    c.Expr[T => GenericRecord](r)
  }

  private def fromGenericRecordInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(tree: Tree, tpe: Type): Tree =
      tpe match {
        case t if t =:= typeOf[Boolean] => q"$tree.asInstanceOf[Boolean]"
        case t if t =:= typeOf[Int]     => q"$tree.asInstanceOf[Int]"
        case t if t =:= typeOf[Long]    => q"$tree.asInstanceOf[Long]"
        case t if t =:= typeOf[Float]   => q"$tree.asInstanceOf[Float]"
        case t if t =:= typeOf[Double]  => q"$tree.asInstanceOf[Double]"
        case t if t =:= typeOf[String]  => q"$tree.toString"

        case t if t =:= typeOf[ByteString] =>
          val bb = q"$tree.asInstanceOf[_root_.java.nio.ByteBuffer]"
          q"_root_.com.google.protobuf.ByteString.copyFrom($bb)"

        case t if t =:= typeOf[Array[Byte]] =>
          val bb = q"$tree.asInstanceOf[_root_.java.nio.ByteBuffer]"
          q"_root_.java.util.Arrays.copyOfRange($bb.array(), $bb.position(), $bb.limit())"

        case t if t.erasure <:< typeOf[scala.collection.Map[String, _]].erasure =>
          map(tree, tpe.typeArgs.tail.head)

        case t if t.erasure =:= typeOf[List[_]].erasure =>
          list(tree, tpe.typeArgs.head)

        case t if isCaseClass(c)(t) =>
          val fn = TermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree.asInstanceOf[${p(c, ApacheAvro)}.generic.GenericRecord]
                ${constructor(t, fn)}
              }
          """
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }

    def option(tree: Tree, tpe: Type): Tree =
      q"if ($tree == null) None else Some(${cast(tree, tpe)})"

    def list(tree: Tree, tpe: Type): Tree = {
      val jl = tq"_root_.java.util.List[AnyRef]"
      q"asScala($tree.asInstanceOf[$jl]).iterator.map(x => ${cast(q"x", tpe)}).toList"
    }

    def map(tree: Tree, tpe: Type): Tree = {
      val jm = tq"_root_.java.util.Map[AnyRef, AnyRef]"
      q"asScala($tree.asInstanceOf[$jm]).iterator.map(kv => (kv._1.toString, ${cast(q"kv._2", tpe)})).toMap"
    }

    def field(symbol: Symbol, fn: TermName): Tree = {
      val name = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$fn.get($name)"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        option(tree, tpe.typeArgs.head)
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
    q"""(r: ${p(c, ApacheAvro)}.generic.GenericRecord) => {
          import _root_.scala.collection.compat.extra.CollectionConverters._
          ${constructor(tpe, tn)}
        }
    """
  }

  private def toGenericRecordInternal(c: blackbox.Context)(tpe: c.Type): c.Tree = {
    import c.universe._

    // =======================================================================
    // Converter helpers
    // =======================================================================

    def cast(tree: Tree, tpe: Type): Tree =
      tpe match {
        case t if t =:= typeOf[Boolean] => tree
        case t if t =:= typeOf[Int]     => tree
        case t if t =:= typeOf[Long]    => tree
        case t if t =:= typeOf[Float]   => tree
        case t if t =:= typeOf[Double]  => tree
        case t if t =:= typeOf[String]  => tree

        case t if t =:= typeOf[ByteString] => q"$tree.asReadOnlyByteBuffer"
        case t if t =:= typeOf[Array[Byte]] =>
          q"_root_.java.nio.ByteBuffer.wrap($tree)"

        case t if t.erasure <:< typeOf[scala.collection.Map[String, _]].erasure =>
          map(tree, tpe.typeArgs.tail.head)

        case t if t.erasure <:< typeOf[List[_]].erasure =>
          list(tree, tpe.typeArgs.head)

        case t if isCaseClass(c)(t) =>
          val fn = TermName("r" + t.typeSymbol.name)
          q"""{
                val $fn = $tree
                ${constructor(t, fn)}
              }
          """
        case _ => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }

    def option(tree: Tree, tpe: Type): Tree =
      q"if ($tree.isDefined) ${cast(q"$tree.get", tpe)} else null"

    def list(tree: Tree, tpe: Type): Tree =
      q"asJava($tree.map(x => ${cast(q"x", tpe)}))"

    def map(tree: Tree, tpe: Type): Tree =
      q"asJava($tree.iterator.map(kv => kv._1 -> ${cast(q"kv._2", tpe)}).toMap)"

    def field(symbol: Symbol, fn: TermName): (String, Tree) = {
      val name = symbol.name.toString
      val fieldName = symbol.name.toString
      val tpe = symbol.asMethod.returnType

      val tree = q"$fn.${TermName(name)}"
      if (tpe.erasure =:= typeOf[Option[_]].erasure) {
        (fieldName, option(tree, tpe.typeArgs.head))
      } else {
        (fieldName, cast(tree, tpe))
      }
    }

    def constructor(tpe: Type, fn: TermName): Tree = {
      val sets = tpe.erasure match {
        case t if isCaseClass(c)(t) => getFields(c)(t).map(s => field(s, fn))
        case _                      => c.abort(c.enclosingPosition, s"Unsupported type: $tpe")
      }
      val schemaOf = q"${p(c, ScioAvroType)}.schemaOf[$tpe]"
      val header =
        q"val result = new ${p(c, ApacheAvro)}.generic.GenericData.Record($schemaOf)"
      val body = sets.map { case (fieldName, value) =>
        q"if (${p(c, ScioAvro)}.types.ConverterUtil.notNull($value)) result.put($fieldName, $value)"
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
  @inline def notNull[@specialized(Boolean, Int, Long, Float, Double) T](x: T): Boolean = x != null
}
