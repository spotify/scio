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

import scala.annotation.experimental
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import scala.reflect.ClassTag

trait ToMacro {
  /**
   * Convert instance of ${T} in this SCollection into instances of ${O} based on the Schemas on the
   * 2 classes. The compatibility of thoses classes is checked at compile time.
   * @see
   *   To#unsafe
   */
  inline def safe[I: Schema, O: Schema]: To[I, O] =
    ${ ToMacro.safeImpl[I, O] }
}

object ToMacro {
  import scala.quoted._
  import scala.compiletime.summonInline

  // TODO migration: not yet implemented
  @experimental
  def safeImpl[I: Type, O: Type](using Quotes): Expr[To[I, O]] = {
    val h = new SchemaMacroHelpers(quotes)
    import h._
    import quotes.reflect._

    val tpeI = TypeRepr.of[I]
    val tpeO = TypeRepr.of[O]

    val sIn = null
    val sOut = null

    // val expr = Expr[(Schema[I], Schema[O])]('{(${untyped(iSchema)}, ${untyped(oSchema)})})
    // val (sIn, sOut) = c.eval(expr) // eval????

    val schemaOut: BSchema = SchemaMaterializer.fieldType(sOut).getRowSchema()
    val schemaIn: BSchema = SchemaMaterializer.fieldType(sIn).getRowSchema()

    To.checkCompatibility(schemaIn, schemaOut) {
      '{ com.spotify.scio.schemas.To.unchecked[I, O](using summonInline[Schema[I]], summonInline[Schema[O]], summonInline[ClassTag[O]]) }
    }.fold(message => report.errorAndAbort(message), t => t)
  }
}