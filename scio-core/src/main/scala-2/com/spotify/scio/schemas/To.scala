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

import com.spotify.scio.values._
import com.spotify.scio.coders._
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}

import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import scala.reflect.ClassTag

trait ToMacro {
  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes. The compatibility of thoses classes is checked
   * at compile time.
   * @see To#unsafe
   */
  def safe[I: Schema, O: Schema]: To[I, O] =
    macro ToMacro.safeImpl[I, O]
}

object ToMacro {
  import scala.reflect.macros._
  def safeImpl[I: c.WeakTypeTag, O: c.WeakTypeTag](
    c: blackbox.Context
  )(iSchema: c.Expr[Schema[I]], oSchema: c.Expr[Schema[O]]): c.Expr[To[I, O]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    val tpeI = weakTypeOf[I]
    val tpeO = weakTypeOf[O]

    val expr = c.Expr[(Schema[I], Schema[O])](q"(${untyped(iSchema)}, ${untyped(oSchema)})")
    val (sIn, sOut) = c.eval(expr)

    val schemaOut: BSchema = SchemaMaterializer.fieldType(sOut).getRowSchema()
    val schemaIn: BSchema = SchemaMaterializer.fieldType(sIn).getRowSchema()

    To.checkCompatibility(schemaIn, schemaOut) {
      q"""_root_.com.spotify.scio.schemas.To.unchecked[$tpeI, $tpeO]"""
    }.fold(message => c.abort(c.enclosingPosition, message), t => c.Expr[To[I, O]](t))
  }
}
