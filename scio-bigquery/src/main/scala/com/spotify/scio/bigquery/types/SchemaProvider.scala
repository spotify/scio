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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.protobuf.ByteString
import com.spotify.scio.bigquery.types.MacroUtil._
import com.spotify.scio.bigquery.validation.{OverrideTypeProvider, OverrideTypeProviderFinder}
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

private[types] object SchemaProvider {

  def schemaOf[T: TypeTag]: TableSchema = {
    val fields = typeOf[T].erasure match {
      case t if isCaseClass(t) => toFields(t)
      case t => throw new RuntimeException(s"Unsupported type $t")
    }
    val r = new TableSchema().setFields(fields.toList.asJava)
    debug(s"SchemaProvider.schemaOf[${typeOf[T]}]:")
    debug(r)
    r
  }

  private def field(mode: String, name: String, tpe: String, desc: Option[String],
                    nested: Iterable[TableFieldSchema]): TableFieldSchema = {
    val s = new TableFieldSchema().setMode(mode).setName(name).setType(tpe)
    if (nested.nonEmpty) {
      s.setFields(nested.toList.asJava)
    }
    desc.foreach(s.setDescription)
    s
  }

  val provider: OverrideTypeProvider = OverrideTypeProviderFinder.getProvider

  // scalastyle:off cyclomatic.complexity
  private def rawType(tpe: Type): (String, Iterable[TableFieldSchema]) = tpe match {
    case t if provider.shouldOverrideType(t) => (provider.getBigQueryType(t), Iterable.empty)
    case t if t =:= typeOf[Boolean] => ("BOOLEAN", Iterable.empty)
    case t if t =:= typeOf[Int] => ("INTEGER", Iterable.empty)
    case t if t =:= typeOf[Long] => ("INTEGER", Iterable.empty)
    case t if t =:= typeOf[Float] => ("FLOAT", Iterable.empty)
    case t if t =:= typeOf[Double]  => ("FLOAT", Iterable.empty)
    case t if t =:= typeOf[String] => ("STRING", Iterable.empty)

    case t if t =:= typeOf[ByteString] => ("BYTES", Iterable.empty)
    case t if t =:= typeOf[Array[Byte]] => ("BYTES", Iterable.empty)

    case t if t =:= typeOf[Instant] => ("TIMESTAMP", Iterable.empty)
    case t if t =:= typeOf[LocalDate] => ("DATE", Iterable.empty)
    case t if t =:= typeOf[LocalTime] => ("TIME", Iterable.empty)
    case t if t =:= typeOf[LocalDateTime] => ("DATETIME", Iterable.empty)

    case t if isCaseClass(t) => ("RECORD", toFields(t))
    case _ => throw new RuntimeException(s"Unsupported type: $tpe")
  }
  // scalastyle:on cyclomatic.complexity

  private def toField(f: (Symbol, Option[String])): TableFieldSchema = {
    val (symbol, desc) = f
    val name = symbol.name.toString
    val tpe = symbol.asMethod.returnType

    val (mode, valType) = tpe match {
      case t if t.erasure =:= typeOf[Option[_]].erasure => ("NULLABLE", tpe.typeArgs.head)
      case t if t.erasure =:= typeOf[List[_]].erasure => ("REPEATED", tpe.typeArgs.head)
      case _ => ("REQUIRED", tpe)
    }
    val (tpeParam, nestedParam) = rawType(valType)
    field(mode, name, tpeParam, desc, nestedParam)
  }

  private def toFields(t: Type): Iterable[TableFieldSchema] = getFields(t).map(toField)

  private def getFields(t: Type): Iterable[(Symbol, Option[String])] =
    t.decls.filter(isField) zip fieldDescs(t)

  private def fieldDescs(t: Type): Iterable[Option[String]] = {
    val tpe = "com.spotify.scio.bigquery.types.description"
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
