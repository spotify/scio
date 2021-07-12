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

import java.util.Collections.{emptyList, emptyMap}
import java.util.concurrent.ConcurrentHashMap
import java.util.function

import com.google.protobuf.ByteString
import com.spotify.scio.avro.types.MacroUtil._
import org.apache.avro.{JsonProperties, Schema}
import org.apache.avro.Schema.Field

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

private[types] object SchemaProvider {
  private[this] val m: ConcurrentHashMap[Type, Schema] =
    new ConcurrentHashMap[Type, Schema]()

  def schemaOf[T: TypeTag]: Schema = {
    val tpe = typeOf[T]

    if (!isCaseClass(tpe.erasure)) {
      throw new RuntimeException(s"Unsupported type $tpe.erasure")
    }

    m.computeIfAbsent(
      tpe,
      new function.Function[Type, Schema] {
        override def apply(t: universe.Type): Schema = toSchema(tpe)._1
      }
    )
  }

  private def toSchema(tpe: Type): (Schema, Any) = tpe match {
    case t if t =:= typeOf[Boolean] =>
      (Schema.create(Schema.Type.BOOLEAN), null)
    case t if t =:= typeOf[Int]    => (Schema.create(Schema.Type.INT), null)
    case t if t =:= typeOf[Long]   => (Schema.create(Schema.Type.LONG), null)
    case t if t =:= typeOf[Float]  => (Schema.create(Schema.Type.FLOAT), null)
    case t if t =:= typeOf[Double] => (Schema.create(Schema.Type.DOUBLE), null)
    case t if t =:= typeOf[String] => (Schema.create(Schema.Type.STRING), null)
    case t if t =:= typeOf[ByteString] =>
      (Schema.create(Schema.Type.BYTES), null)
    case t if t =:= typeOf[Array[Byte]] =>
      (Schema.create(Schema.Type.BYTES), null)

    case t if t.erasure =:= typeOf[Option[_]].erasure =>
      val s = toSchema(t.typeArgs.head)._1
      (Schema.createUnion(Schema.create(Schema.Type.NULL), s), JsonProperties.NULL_VALUE)

    case t if t.erasure <:< typeOf[scala.collection.Map[String, _]].erasure =>
      val s = toSchema(t.typeArgs.tail.head)._1
      (Schema.createMap(s), emptyMap())

    case t if t.erasure <:< typeOf[List[_]].erasure =>
      val s = toSchema(t.typeArgs.head)._1
      (Schema.createArray(s), emptyList())

    case t if isCaseClass(t) =>
      val fields = toFields(t)
      val doc = recordDoc(t)
      val name = t.typeSymbol.name.toString.split("\\$").head
      val pkg = t.typeSymbol.owner.fullName
      (Schema.createRecord(name, doc.orNull, pkg, false, fields.asJava), null)

    case _ => throw new RuntimeException(s"Unsupported type: $tpe")
  }

  private def toField(f: (Symbol, Option[String])): Field = {
    val (symbol, doc) = f
    val name = symbol.name.toString
    val tpe = symbol.asMethod.returnType
    val (schema, default) = toSchema(tpe)
    new Field(name, schema, doc.orNull, default)
  }

  private def toFields(t: Type): List[Field] =
    getFields(t).iterator.map(toField).toList

  private def getFields(t: Type): Iterable[(Symbol, Option[String])] =
    t.decls.filter(isField) zip fieldDoc(t)

  private def recordDoc(t: Type): Option[String] = {
    val tpe = "com.spotify.scio.avro.types.doc"
    t.typeSymbol.annotations
      .find(_.tree.tpe.toString == tpe)
      .map { a =>
        val q"new $t($v)" = a.tree
        val Literal(Constant(s)) = v
        s.toString
      }
  }

  private def fieldDoc(t: Type): Iterable[Option[String]] = {
    val tpe = "com.spotify.scio.avro.types.doc"
    t.typeSymbol.asClass.primaryConstructor.typeSignature.paramLists.head.map {
      _.annotations
        .find(_.tree.tpe.toString == tpe)
        .map { a =>
          val q"new $t($v)" = a.tree
          val Literal(Constant(s)) = v
          s.toString
        }
    }
  }
}
