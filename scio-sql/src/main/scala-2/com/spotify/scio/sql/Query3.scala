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

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !! generated with sql.py
// !! DO NOT EDIT MANUALLY
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

package com.spotify.scio.sql

import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.ParseException
import org.apache.beam.sdk.values._

import scala.reflect.ClassTag

final case class Query3[A, B, C, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  udfs: List[Udf] = Nil
)

object Query3 {
  import scala.reflect.macros.blackbox
  import QueryMacros._

  def typecheck[A: Schema, B: Schema, C: Schema, R: Schema](
    q: Query3[A, B, C, R]
  ): Either[String, Query3[A, B, C, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C]
  ): Query3[A, B, C, R] =
    macro typed3Impl[A, B, C, R]

  def typed3Impl[A: c.WeakTypeTag, B: c.WeakTypeTag, C: c.WeakTypeTag, R: c.WeakTypeTag](
    c: blackbox.Context
  )(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query3[A, B, C, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[R](c)

    val (schemas1, schemas2, schemas3, schemas4) =
      c.eval(
        c.Expr[(Schema[A], Schema[B], Schema[C], Schema[R])](
          q"(${untyped(aSchema)}, ${untyped(bSchema)}, ${untyped(cSchema)}, ${untyped(rSchema)})"
        )
      )

    val sq =
      Query3[A, B, C, R](cons(c)(query), tupleTag(c)(aTag), tupleTag(c)(bTag), tupleTag(c)(cTag))
    typecheck(sq)(schemas1, schemas2, schemas3, schemas4)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query3[A, B, C, R]](
            q"_root_.com.spotify.scio.sql.Query3($query, $aTag, $bTag, $cTag)"
          )
      )
  }
}

final class SqlSCollection3[A: Schema: ClassTag, B: Schema: ClassTag, C: Schema: ClassTag](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C]
) {

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query3(q, aTag, bTag, cTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def query(q: Query3[A, B, C, Row]): SCollection[Row] =
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .apply(s"${collA.tfName} join ${collB.tfName} join ${collC.tfName}", sqlTransform)

    }

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def queryAs[R: Schema: ClassTag](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query3(q, aTag, bTag, cTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def queryAs[R: Schema: ClassTag](q: Query3[A, B, C, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query3.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
