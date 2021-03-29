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

package com.spotify.scio.schemas.Schema
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}

import scala.compiletime._
import scala.deriving._
import scala.quoted._


object ToMacro {

  def interpretSchema[T: Type](schemaExpr: Expr[Schema[T]])(using Quotes): Option[Schema[T]] =
    schemaExpr match
    case '{ Schema.optionSchema[t](using $tSchemaExpr) } =>
      for
        tSchema <- interpretSchema(tSchemaExpr)
      yield
        Schema.optionSchema(using tSchema).asInstanceOf[Schema[T]]
    case '{ Schema.mapSchema[k, v](using $keySchemaExpr, $valueSchemaExpr) } =>
      for 
        keySchema <- interpretSchema(keySchemaExpr)
        valueSchema <- interpretSchema(valueSchemaExpr)
      yield Schema.mapSchema(using keySchema, valueSchema).asInstanceOf[Schema[T]]
    case _ => None


  def safeImpl[I, O](si: Expr[Schema[I]])(implicit q: Quotes): Expr[To[I, O]] = {
    ???
  }
}

trait ToMacro {
  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes. The compatibility of thoses classes is checked
   * at compile time.
   * @see To#unsafe
   */
  // TODO: scala3
  inline def safe[I, O](inline si: Schema[I], inline so: Schema[O]): To[I, O] =
    ???
}
