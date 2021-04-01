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

import scala.compiletime._
import scala.deriving._
import scala.quoted._


object ToMacro {

  def interpretSchema[T: scala.quoted.Type](schemaExpr: Expr[Schema[T]])(using Quotes): Option[Schema[T]] = schemaExpr match {
    case '{ Schema.stringSchema }     => Some(Schema.stringSchema.asInstanceOf[Schema[T]])
    case '{ Schema.byteSchema }       => Some(Schema.byteSchema.asInstanceOf[Schema[T]])
    case '{ Schema.bytesSchema }      => Some(Schema.bytesSchema.asInstanceOf[Schema[T]])
    case '{ Schema.sortSchema }       => Some(Schema.sortSchema.asInstanceOf[Schema[T]])
    case '{ Schema.intSchema }        => Some(Schema.intSchema.asInstanceOf[Schema[T]])
    case '{ Schema.longSchema }       => Some(Schema.longSchema.asInstanceOf[Schema[T]])
    case '{ Schema.floatSchema }      => Some(Schema.floatSchema.asInstanceOf[Schema[T]])
    case '{ Schema.doubleSchema }     => Some(Schema.doubleSchema.asInstanceOf[Schema[T]])
    case '{ Schema.bigDecimalSchema } => Some(Schema.bigDecimalSchema.asInstanceOf[Schema[T]])
    case '{ Schema.booleanSchema }    => Some(Schema.booleanSchema.asInstanceOf[Schema[T]])

    case '{ Schema.optionSchema[t](using $tSchemaExpr) } =>
      for (tSchema <- interpretSchema(tSchemaExpr))
      yield Schema.optionSchema(using tSchema).asInstanceOf[Schema[T]]

    case '{ Schema.mapSchema[k, v](using $keySchemaExpr, $valueSchemaExpr) } =>
      for {
        keySchema <- interpretSchema(keySchemaExpr)
        valueSchema <- interpretSchema(valueSchemaExpr)
      } yield Schema.mapSchema(using keySchema, valueSchema).asInstanceOf[Schema[T]]

    case _ => None
  }


  def safeImpl[I, O](iSchema: Expr[Schema[I]], oSchema: Expr[Schema[O]])(using Quotes): Expr[To[I, O]] = {
    import scala.quoted.quotes.reflect.report

    (interpretSchema(iSchema), interpretSchema(oSchema)) match {
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
        To.checkCompatibility(schemaIn, schemaOut)('{ To.unchecked[I, O] })
          .fold(message => report.throwError(message), identity)
    }
  }
}

trait ToMacro {
  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes. The compatibility of thoses classes is checked
   * at compile time.
   * @see To#unsafe
   */
  inline def safe[I, O](inline iSchema: Schema[I], inline oSchema: Schema[O]): To[I, O] = 
    ${ ToMacro.safeImpl('iSchema, 'oSchema) }
}
