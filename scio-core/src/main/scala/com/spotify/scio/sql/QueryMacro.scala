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

    println(iRecord.tree.tpe.member(TypeName("Repr")).typeSignature.dealias)
    println(oSchema.tree.tpe.member(TypeName("Repr")).typeSignature.dealias)
    println(s"Query was: $s")
    ???
  }

  // import com.spotify.scio.sql._
  // case class Foo(i: Int, s: String)
  // Query.tsql[Foo, (Int, String)]("select * from yolo")
}
