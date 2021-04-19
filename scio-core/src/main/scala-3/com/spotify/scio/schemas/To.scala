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

package com.spotify.scio.schemas

import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}
import BSchema.{ FieldType => BFieldType }

import scala.compiletime._
import scala.deriving._
import scala.quoted._
import scala.reflect.ClassTag
import scala.collection.mutable

object ToMacro {

  def safeImpl[I: scala.quoted.Type, O: scala.quoted.Type](
    iSchema: Expr[Schema[I]],
    oSchema: Expr[Schema[O]]
  )(using Quotes): Expr[To[I, O]] = {
    import scala.quoted.quotes.reflect.{report, TypeRepr}

    (interpret[I] , interpret[O]) match {
      case (None, None) => report.throwError(
        s"""
        |Could not interpret input schema:
        |  ${iSchema.show}
        |Could not interpret output schema:
        |  ${oSchema.show}
        |""".stripMargin
      )
      case (None, _) => report.throwError("Could not interpret input schema: " + iSchema.show)
      case (_, None) => report.throwError("Could not interpret output schema: " + oSchema.show)
      case (Some(sIn), Some(sOut)) =>
        val schemaOut: BSchema = SchemaMaterializer.fieldType(sOut).getRowSchema()
        val schemaIn: BSchema = SchemaMaterializer.fieldType(sIn).getRowSchema()
        val classTagOpt = Expr.summon[ClassTag[O]]
        if (classTagOpt.isEmpty) {
          report.throwError(s"Could not summon Expr[ClassTag[${TypeRepr.of[O].show}]]")
        }
        val classTag = classTagOpt.get
        To.checkCompatibility(schemaIn, schemaOut)('{ To.unchecked[I, O](using $iSchema, $oSchema, $classTag) })
          .fold(message => report.throwError(message), identity)
    }
  }

  private def sequence[T](ls: List[Option[T]]): Option[List[T]] =
    if ls.exists(_.isEmpty) then None
    else Some(ls.collect { case Some(x) => x })

  private def interpret[T: scala.quoted.Type](using Quotes): Option[Schema[T]] = 
    Type.of[T] match {
      case '[java.lang.Byte]        => Some(Schema.jByteSchema.asInstanceOf[Schema[T]])
      case '[Array[java.lang.Byte]] => Some(Schema.jBytesSchema.asInstanceOf[Schema[T]])
      case '[java.lang.Short]       => Some(Schema.jShortSchema.asInstanceOf[Schema[T]])
      case '[java.lang.Integer]     => Some(Schema.jIntegerSchema.asInstanceOf[Schema[T]])
      case '[java.lang.Long]        => Some(Schema.jLongSchema.asInstanceOf[Schema[T]])
      case '[java.lang.Float]       => Some(Schema.jFloatSchema.asInstanceOf[Schema[T]])
      case '[java.lang.Double]      => Some(Schema.jDoubleSchema.asInstanceOf[Schema[T]])
      case '[java.math.BigDecimal]  => Some(Schema.jBigDecimalSchema.asInstanceOf[Schema[T]])
      case '[java.lang.Boolean]     => Some(Schema.jBooleanSchema.asInstanceOf[Schema[T]])
      case '[java.util.List[u]] =>
        for (itemSchema) <- interpret[u]
        yield Schema.jListSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[java.util.ArrayList[u]] =>
        for (itemSchema) <- interpret[u]
        yield Schema.jArrayListSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[java.util.Map[k, v]] =>
        for {
          keySchema <- interpret[k]
          valueSchema <- interpret[v]
        } yield Schema.jMapSchema(using keySchema, valueSchema).asInstanceOf[Schema[T]]
      // TODO javaBeanSchema
      // TODO javaEnumSchema
      case '[java.time.LocalDate] => Some(Schema.jLocalDate.asInstanceOf[Schema[T]])

      case '[String]      => Some(Schema.stringSchema.asInstanceOf[Schema[T]])
      case '[Byte]        => Some(Schema.byteSchema.asInstanceOf[Schema[T]])
      case '[Array[Byte]] => Some(Schema.bytesSchema.asInstanceOf[Schema[T]])
      case '[Short]       => Some(Schema.sortSchema.asInstanceOf[Schema[T]])
      case '[Int]         => Some(Schema.intSchema.asInstanceOf[Schema[T]])
      case '[Long]        => Some(Schema.longSchema.asInstanceOf[Schema[T]])
      case '[Float]       => Some(Schema.floatSchema.asInstanceOf[Schema[T]])
      case '[Double]      => Some(Schema.doubleSchema.asInstanceOf[Schema[T]])
      case '[BigDecimal]  => Some(Schema.bigDecimalSchema.asInstanceOf[Schema[T]])
      case '[Boolean]     => Some(Schema.booleanSchema.asInstanceOf[Schema[T]])
      case '[Option[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.optionSchema(using itemSchema).asInstanceOf[Schema[T]]
      // TODO Array[T]
      case '[List[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.listSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[Seq[u]] => 
        for (itemSchema <- interpret[u])
        yield Schema.seqSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[TraversableOnce[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.traversableOnceSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[Iterable[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.iterableSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[mutable.ArrayBuffer[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.arrayBufferSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[mutable.Buffer[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.bufferSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[Set[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.setSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[mutable.Set[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.mutableSetSchema(using itemSchema).asInstanceOf[Schema[T]]
      // TODO SortedSet[T]
      case '[mutable.ListBuffer[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.listBufferSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[Vector[u]] =>
        for (itemSchema <- interpret[u])
        yield Schema.vectorSchema(using itemSchema).asInstanceOf[Schema[T]]
      case '[Map[k, v]] =>
        for {
          keySchema <- interpret[k]
          valueSchema <- interpret[v]
        } yield Schema.mapSchema(using keySchema, valueSchema).asInstanceOf[Schema[T]]
      case '[mutable.Map[k, v]] =>
        for {
          keySchema <- interpret[k]
          valueSchema <- interpret[v]
        } yield Schema.mutableMapSchema(using keySchema, valueSchema).asInstanceOf[Schema[T]]
      case _ =>
        import quotes.reflect._
        val tp = TypeRepr.of[T]
        val caseClass: Symbol = tp.typeSymbol
        val fields: List[Symbol] = caseClass.caseFields

        // if case class iterate and recurse, else sorry
        if tp <:< TypeRepr.of[Product] && fields.nonEmpty then {
          val schemasOpt: List[Option[(String, Schema[Any])]] = fields.map { (f: Symbol) =>
            assert(f.isValDef)
            val fieldName = f.name
            val fieldType: TypeRepr = tp.memberType(f)
            fieldType.asType match {
              // mattern match to create a bind <3
              case '[u] => interpret[u].asInstanceOf[Option[Schema[Any]]].map(s => (fieldName, s))
            }
          }
          sequence(schemasOpt).map(schemas => Record(schemas.toArray, null, null))
        } else None
    }
}

trait ToMacro {
  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes. The compatibility of thoses classes is checked
   * at compile time.
   * @see To#unsafe
   */
  inline def safe[I, O](using inline iSchema: Schema[I], inline oSchema: Schema[O]): To[I, O] = 
    ${ ToMacro.safeImpl('iSchema, 'oSchema) }
}
