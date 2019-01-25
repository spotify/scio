/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.sql

import scala.reflect.macros.blackbox
import com.spotify.scio.schemas._
import org.apache.beam.sdk.schemas.{Schema => BSchema}

private[sql] object QueryMacros {

  def typecheck[I: c.WeakTypeTag, O: c.WeakTypeTag](c: blackbox.Context)(query: c.Expr[String])(
    iRecord: c.Expr[Record[I]],
    oSchema: c.Expr[Schema[O]]): c.Expr[Query[I, O]] = {
    import c.universe._
    val typeOfI = weakTypeOf[I]
    val q"${s: String}" = query.tree
    // val q"UnliftableSchema(${schema: String})" = iRecord.tree
    // println(schema)
    // val q"${arr: Array[Int]}" = q"Array[Int]()"
    println(iRecord.tree.tpe.member(TypeName("Repr")).typeSignature)
    println(oSchema.tree.tpe.member(TypeName("Repr")).typeSignature)
    println(s"Query was: $s")
    ???
  }

}
