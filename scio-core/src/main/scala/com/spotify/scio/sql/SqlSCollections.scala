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
// !! generated with tuplecoders.py
// !! DO NOT EDIT MANUALLY
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

package com.spotify.scio.sql

import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.ParseException
import org.apache.beam.sdk.values._

final case class Query2[A, B, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  udfs: List[Udf] = Nil
)

final class SqlSCollection2[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]) {

  def query(q: String, aTag: TupleTag[A], bTag: TupleTag[B], udfs: Udf*): SCollection[Row] =
    query(Query2(q, aTag, bTag, udfs.toList))

  def query(q: Query2[A, B, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .apply(s"{collA.tfName} join {collB.tfName}", sqlTransform)

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query2(q, aTag, bTag, udfs.toList))

  def queryAs[R: Schema](q: Query2[A, B, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query3[A, B, C, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  udfs: List[Udf] = Nil
)

final class SqlSCollection3[A: Schema, B: Schema, C: Schema](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query3(q, aTag, bTag, cTag, udfs.toList))

  def query(q: Query3[A, B, C, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .apply(s"{collA.tfName} join {collB.tfName} join {collC.tfName}", sqlTransform)

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query3(q, aTag, bTag, cTag, udfs.toList))

  def queryAs[R: Schema](q: Query3[A, B, C, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query4[A, B, C, D, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  udfs: List[Udf] = Nil
)

final class SqlSCollection4[A: Schema, B: Schema, C: Schema, D: Schema](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query4(q, aTag, bTag, cTag, dTag, udfs.toList))

  def query(q: Query4[A, B, C, D, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .apply(
          s"{collA.tfName} join {collB.tfName} join {collC.tfName} join {collD.tfName}",
          sqlTransform
        )

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query4(q, aTag, bTag, cTag, dTag, udfs.toList))

  def queryAs[R: Schema](q: Query4[A, B, C, D, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query5[A, B, C, D, E, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  udfs: List[Udf] = Nil
)

final class SqlSCollection5[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query5(q, aTag, bTag, cTag, dTag, eTag, udfs.toList))

  def query(q: Query5[A, B, C, D, E, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .apply(
          s"{collA.tfName} join {collB.tfName} join {collC.tfName} join {collD.tfName} join {collE.tfName}",
          sqlTransform
        )

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query5(q, aTag, bTag, cTag, dTag, eTag, udfs.toList))

  def queryAs[R: Schema](q: Query5[A, B, C, D, E, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.eTag, q.udfs: _*)
        .to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query6[A, B, C, D, E, F, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  udfs: List[Udf] = Nil
)

final class SqlSCollection6[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query6(q, aTag, bTag, cTag, dTag, eTag, fTag, udfs.toList))

  def query(q: Query6[A, B, C, D, E, F, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .and(q.fTag, collF.internal)
        .apply(
          s"{collA.tfName} join {collB.tfName} join {collC.tfName} join {collD.tfName} join {collE.tfName} join {collF.tfName}",
          sqlTransform
        )

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query6(q, aTag, bTag, cTag, dTag, eTag, fTag, udfs.toList))

  def queryAs[R: Schema](q: Query6[A, B, C, D, E, F, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.eTag, q.fTag, q.udfs: _*)
        .to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query7[A, B, C, D, E, F, G, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  gTag: TupleTag[G],
  udfs: List[Udf] = Nil
)

final class SqlSCollection7[
  A: Schema,
  B: Schema,
  C: Schema,
  D: Schema,
  E: Schema,
  F: Schema,
  G: Schema
](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F],
  g: SCollection[G]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query7(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, udfs.toList))

  def query(q: Query7[A, B, C, D, E, F, G, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val collG = Sql.setSchema(g)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .and(q.fTag, collF.internal)
        .and(q.gTag, collG.internal)
        .apply(
          s"{collA.tfName} join {collB.tfName} join {collC.tfName} join {collD.tfName} join {collE.tfName} join {collF.tfName} join {collG.tfName}",
          sqlTransform
        )

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query7(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, udfs.toList))

  def queryAs[R: Schema](q: Query7[A, B, C, D, E, F, G, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.eTag, q.fTag, q.gTag, q.udfs: _*)
        .to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query8[A, B, C, D, E, F, G, H, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  gTag: TupleTag[G],
  hTag: TupleTag[H],
  udfs: List[Udf] = Nil
)

final class SqlSCollection8[
  A: Schema,
  B: Schema,
  C: Schema,
  D: Schema,
  E: Schema,
  F: Schema,
  G: Schema,
  H: Schema
](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F],
  g: SCollection[G],
  h: SCollection[H]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query8(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, udfs.toList))

  def query(q: Query8[A, B, C, D, E, F, G, H, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val collG = Sql.setSchema(g)
      val collH = Sql.setSchema(h)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .and(q.fTag, collF.internal)
        .and(q.gTag, collG.internal)
        .and(q.hTag, collH.internal)
        .apply(
          s"{collA.tfName} join {collB.tfName} join {collC.tfName} join {collD.tfName} join {collE.tfName} join {collF.tfName} join {collG.tfName} join {collH.tfName}",
          sqlTransform
        )

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query8(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, udfs.toList))

  def queryAs[R: Schema](q: Query8[A, B, C, D, E, F, G, H, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.eTag, q.fTag, q.gTag, q.hTag, q.udfs: _*)
        .to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query9[A, B, C, D, E, F, G, H, I, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  gTag: TupleTag[G],
  hTag: TupleTag[H],
  iTag: TupleTag[I],
  udfs: List[Udf] = Nil
)

final class SqlSCollection9[
  A: Schema,
  B: Schema,
  C: Schema,
  D: Schema,
  E: Schema,
  F: Schema,
  G: Schema,
  H: Schema,
  I: Schema
](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F],
  g: SCollection[G],
  h: SCollection[H],
  i: SCollection[I]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query9(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, udfs.toList))

  def query(q: Query9[A, B, C, D, E, F, G, H, I, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val collG = Sql.setSchema(g)
      val collH = Sql.setSchema(h)
      val collI = Sql.setSchema(i)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .and(q.fTag, collF.internal)
        .and(q.gTag, collG.internal)
        .and(q.hTag, collH.internal)
        .and(q.iTag, collI.internal)
        .apply(
          s"{collA.tfName} join {collB.tfName} join {collC.tfName} join {collD.tfName} join {collE.tfName} join {collF.tfName} join {collG.tfName} join {collH.tfName} join {collI.tfName}",
          sqlTransform
        )

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query9(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, udfs.toList))

  def queryAs[R: Schema](q: Query9[A, B, C, D, E, F, G, H, I, R]): SCollection[R] =
    try {
      query(
        q.query,
        q.aTag,
        q.bTag,
        q.cTag,
        q.dTag,
        q.eTag,
        q.fTag,
        q.gTag,
        q.hTag,
        q.iTag,
        q.udfs: _*
      ).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query10[A, B, C, D, E, F, G, H, I, J, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  gTag: TupleTag[G],
  hTag: TupleTag[H],
  iTag: TupleTag[I],
  jTag: TupleTag[J],
  udfs: List[Udf] = Nil
)

final class SqlSCollection10[
  A: Schema,
  B: Schema,
  C: Schema,
  D: Schema,
  E: Schema,
  F: Schema,
  G: Schema,
  H: Schema,
  I: Schema,
  J: Schema
](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F],
  g: SCollection[G],
  h: SCollection[H],
  i: SCollection[I],
  j: SCollection[J]
) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    jTag: TupleTag[J],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query10(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, jTag, udfs.toList))

  def query(q: Query10[A, B, C, D, E, F, G, H, I, J, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val collG = Sql.setSchema(g)
      val collH = Sql.setSchema(h)
      val collI = Sql.setSchema(i)
      val collJ = Sql.setSchema(j)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .and(q.fTag, collF.internal)
        .and(q.gTag, collG.internal)
        .and(q.hTag, collH.internal)
        .and(q.iTag, collI.internal)
        .and(q.jTag, collJ.internal)
        .apply(
          s"{collA.tfName} join {collB.tfName} join {collC.tfName} join {collD.tfName} join {collE.tfName} join {collF.tfName} join {collG.tfName} join {collH.tfName} join {collI.tfName} join {collJ.tfName}",
          sqlTransform
        )

    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    jTag: TupleTag[J],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query10(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, jTag, udfs.toList))

  def queryAs[R: Schema](q: Query10[A, B, C, D, E, F, G, H, I, J, R]): SCollection[R] =
    try {
      query(
        q.query,
        q.aTag,
        q.bTag,
        q.cTag,
        q.dTag,
        q.eTag,
        q.fTag,
        q.gTag,
        q.hTag,
        q.iTag,
        q.jTag,
        q.udfs: _*
      ).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        QueriesGen.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

trait SqlGen {
  def from[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]): SqlSCollection2[A, B] =
    new SqlSCollection2(a, b)
  def from[A: Schema, B: Schema, C: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C]
  ): SqlSCollection3[A, B, C] = new SqlSCollection3(a, b, c)
  def from[A: Schema, B: Schema, C: Schema, D: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D]
  ): SqlSCollection4[A, B, C, D] = new SqlSCollection4(a, b, c, d)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E]
  ): SqlSCollection5[A, B, C, D, E] = new SqlSCollection5(a, b, c, d, e)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F]
  ): SqlSCollection6[A, B, C, D, E, F] = new SqlSCollection6(a, b, c, d, e, f)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, G: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G]
  ): SqlSCollection7[A, B, C, D, E, F, G] = new SqlSCollection7(a, b, c, d, e, f, g)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, G: Schema, H: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G],
    h: SCollection[H]
  ): SqlSCollection8[A, B, C, D, E, F, G, H] = new SqlSCollection8(a, b, c, d, e, f, g, h)
  def from[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema
  ](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G],
    h: SCollection[H],
    i: SCollection[I]
  ): SqlSCollection9[A, B, C, D, E, F, G, H, I] = new SqlSCollection9(a, b, c, d, e, f, g, h, i)
  def from[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    J: Schema
  ](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G],
    h: SCollection[H],
    i: SCollection[I],
    j: SCollection[J]
  ): SqlSCollection10[A, B, C, D, E, F, G, H, I, J] =
    new SqlSCollection10(a, b, c, d, e, f, g, h, i, j)
}
object SqlGen extends SqlGen

trait QueriesGen {
  import scala.language.experimental.macros

  def typecheck[A: Schema, B: Schema, R: Schema](
    q: Query2[A, B, R]
  ): Either[String, Query2[A, B, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B]
  ): Query2[A, B, R] =
    macro QueryMacrosGen.typed2Impl[A, B, R]

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
    macro QueryMacrosGen.typed3Impl[A, B, C, R]

  def typecheck[A: Schema, B: Schema, C: Schema, D: Schema, R: Schema](
    q: Query4[A, B, C, D, R]
  ): Either[String, Query4[A, B, C, D, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, D: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D]
  ): Query4[A, B, C, D, R] =
    macro QueryMacrosGen.typed4Impl[A, B, C, D, R]

  def typecheck[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, R: Schema](
    q: Query5[A, B, C, D, E, R]
  ): Either[String, Query5[A, B, C, D, E, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E]
  ): Query5[A, B, C, D, E, R] =
    macro QueryMacrosGen.typed5Impl[A, B, C, D, E, R]

  def typecheck[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, R: Schema](
    q: Query6[A, B, C, D, E, F, R]
  ): Either[String, Query6[A, B, C, D, E, F, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E]),
          (q.fTag.getId, SchemaMaterializer.beamSchema[F])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F]
  ): Query6[A, B, C, D, E, F, R] =
    macro QueryMacrosGen.typed6Impl[A, B, C, D, E, F, R]

  def typecheck[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    R: Schema
  ](q: Query7[A, B, C, D, E, F, G, R]): Either[String, Query7[A, B, C, D, E, F, G, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E]),
          (q.fTag.getId, SchemaMaterializer.beamSchema[F]),
          (q.gTag.getId, SchemaMaterializer.beamSchema[G])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, G: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G]
  ): Query7[A, B, C, D, E, F, G, R] =
    macro QueryMacrosGen.typed7Impl[A, B, C, D, E, F, G, R]

  def typecheck[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    R: Schema
  ](q: Query8[A, B, C, D, E, F, G, H, R]): Either[String, Query8[A, B, C, D, E, F, G, H, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E]),
          (q.fTag.getId, SchemaMaterializer.beamSchema[F]),
          (q.gTag.getId, SchemaMaterializer.beamSchema[G]),
          (q.hTag.getId, SchemaMaterializer.beamSchema[H])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    R: Schema
  ](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H]
  ): Query8[A, B, C, D, E, F, G, H, R] =
    macro QueryMacrosGen.typed8Impl[A, B, C, D, E, F, G, H, R]

  def typecheck[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    R: Schema
  ](q: Query9[A, B, C, D, E, F, G, H, I, R]): Either[String, Query9[A, B, C, D, E, F, G, H, I, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E]),
          (q.fTag.getId, SchemaMaterializer.beamSchema[F]),
          (q.gTag.getId, SchemaMaterializer.beamSchema[G]),
          (q.hTag.getId, SchemaMaterializer.beamSchema[H]),
          (q.iTag.getId, SchemaMaterializer.beamSchema[I])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    R: Schema
  ](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I]
  ): Query9[A, B, C, D, E, F, G, H, I, R] =
    macro QueryMacrosGen.typed9Impl[A, B, C, D, E, F, G, H, I, R]

  def typecheck[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    J: Schema,
    R: Schema
  ](
    q: Query10[A, B, C, D, E, F, G, H, I, J, R]
  ): Either[String, Query10[A, B, C, D, E, F, G, H, I, J, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E]),
          (q.fTag.getId, SchemaMaterializer.beamSchema[F]),
          (q.gTag.getId, SchemaMaterializer.beamSchema[G]),
          (q.hTag.getId, SchemaMaterializer.beamSchema[H]),
          (q.iTag.getId, SchemaMaterializer.beamSchema[I]),
          (q.jTag.getId, SchemaMaterializer.beamSchema[J])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    J: Schema,
    R: Schema
  ](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    jTag: TupleTag[J]
  ): Query10[A, B, C, D, E, F, G, H, I, J, R] =
    macro QueryMacrosGen.typed10Impl[A, B, C, D, E, F, G, H, I, J, R]

}
object QueriesGen extends QueriesGen

trait QueryMacrosGen {
  import scala.reflect.macros.blackbox
  import QueryMacrosUtil._

  def typed2Impl[A: c.WeakTypeTag, B: c.WeakTypeTag, R: c.WeakTypeTag](
    c: blackbox.Context
  )(query: c.Expr[String], aTag: c.Expr[TupleTag[A]], bTag: c.Expr[TupleTag[B]])(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query2[A, B, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[R](c)

    val schemas: (Schema[A], Schema[B], Schema[R]) = c.eval(
      c.Expr(q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[R]})")
    )

    val sq = Query2[A, B, R](cons(c)(query), tupleTag(c)(aTag), tupleTag(c)(bTag))
    QueriesGen
      .typecheck(sq)(schemas._1, schemas._2, schemas._3)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ => c.Expr[Query2[A, B, R]](q"_root_.com.spotify.scio.sql.Query2($query, $aTag, $bTag)")
      )
  }

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

    val schemas: (Schema[A], Schema[B], Schema[C], Schema[R]) = c.eval(
      c.Expr(
        q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[R]})"
      )
    )

    val sq =
      Query3[A, B, C, R](cons(c)(query), tupleTag(c)(aTag), tupleTag(c)(bTag), tupleTag(c)(cTag))
    QueriesGen
      .typecheck(sq)(schemas._1, schemas._2, schemas._3, schemas._4)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query3[A, B, C, R]](
            q"_root_.com.spotify.scio.sql.Query3($query, $aTag, $bTag, $cTag)"
          )
      )
  }

  def typed4Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query4[A, B, C, D, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[R](c)

    val schemas: (Schema[A], Schema[B], Schema[C], Schema[D], Schema[R]) = c.eval(
      c.Expr(
        q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[D]}, ${inferImplicitSchema[R]})"
      )
    )

    val sq = Query4[A, B, C, D, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag)
    )
    QueriesGen
      .typecheck(sq)(schemas._1, schemas._2, schemas._3, schemas._4, schemas._5)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query4[A, B, C, D, R]](
            q"_root_.com.spotify.scio.sql.Query4($query, $aTag, $bTag, $cTag, $dTag)"
          )
      )
  }

  def typed5Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query5[A, B, C, D, E, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[R](c)

    val schemas: (Schema[A], Schema[B], Schema[C], Schema[D], Schema[E], Schema[R]) = c.eval(
      c.Expr(
        q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[
          D
        ]}, ${inferImplicitSchema[E]}, ${inferImplicitSchema[R]})"
      )
    )

    val sq = Query5[A, B, C, D, E, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag)
    )
    QueriesGen
      .typecheck(sq)(schemas._1, schemas._2, schemas._3, schemas._4, schemas._5, schemas._6)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query5[A, B, C, D, E, R]](
            q"_root_.com.spotify.scio.sql.Query5($query, $aTag, $bTag, $cTag, $dTag, $eTag)"
          )
      )
  }

  def typed6Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    F: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]],
    fTag: c.Expr[TupleTag[F]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    fSchema: c.Expr[Schema[F]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query6[A, B, C, D, E, F, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[F](c)
    assertConcrete[R](c)

    val schemas: (Schema[A], Schema[B], Schema[C], Schema[D], Schema[E], Schema[F], Schema[R]) =
      c.eval(
        c.Expr(
          q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[
            D
          ]}, ${inferImplicitSchema[E]}, ${inferImplicitSchema[F]}, ${inferImplicitSchema[R]})"
        )
      )

    val sq = Query6[A, B, C, D, E, F, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag),
      tupleTag(c)(fTag)
    )
    QueriesGen
      .typecheck(sq)(
        schemas._1,
        schemas._2,
        schemas._3,
        schemas._4,
        schemas._5,
        schemas._6,
        schemas._7
      )
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query6[A, B, C, D, E, F, R]](
            q"_root_.com.spotify.scio.sql.Query6($query, $aTag, $bTag, $cTag, $dTag, $eTag, $fTag)"
          )
      )
  }

  def typed7Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    F: c.WeakTypeTag,
    G: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]],
    fTag: c.Expr[TupleTag[F]],
    gTag: c.Expr[TupleTag[G]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    fSchema: c.Expr[Schema[F]],
    gSchema: c.Expr[Schema[G]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query7[A, B, C, D, E, F, G, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[F](c)
    assertConcrete[G](c)
    assertConcrete[R](c)

    val schemas
      : (Schema[A], Schema[B], Schema[C], Schema[D], Schema[E], Schema[F], Schema[G], Schema[R]) =
      c.eval(
        c.Expr(
          q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[
            D
          ]}, ${inferImplicitSchema[E]}, ${inferImplicitSchema[F]}, ${inferImplicitSchema[G]}, ${inferImplicitSchema[R]})"
        )
      )

    val sq = Query7[A, B, C, D, E, F, G, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag),
      tupleTag(c)(fTag),
      tupleTag(c)(gTag)
    )
    QueriesGen
      .typecheck(sq)(
        schemas._1,
        schemas._2,
        schemas._3,
        schemas._4,
        schemas._5,
        schemas._6,
        schemas._7,
        schemas._8
      )
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query7[A, B, C, D, E, F, G, R]](
            q"_root_.com.spotify.scio.sql.Query7($query, $aTag, $bTag, $cTag, $dTag, $eTag, $fTag, $gTag)"
          )
      )
  }

  def typed8Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    F: c.WeakTypeTag,
    G: c.WeakTypeTag,
    H: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]],
    fTag: c.Expr[TupleTag[F]],
    gTag: c.Expr[TupleTag[G]],
    hTag: c.Expr[TupleTag[H]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    fSchema: c.Expr[Schema[F]],
    gSchema: c.Expr[Schema[G]],
    hSchema: c.Expr[Schema[H]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query8[A, B, C, D, E, F, G, H, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[F](c)
    assertConcrete[G](c)
    assertConcrete[H](c)
    assertConcrete[R](c)

    val schemas: (
      Schema[A],
      Schema[B],
      Schema[C],
      Schema[D],
      Schema[E],
      Schema[F],
      Schema[G],
      Schema[H],
      Schema[R]
    ) = c.eval(
      c.Expr(
        q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[D]}, ${inferImplicitSchema[
          E
        ]}, ${inferImplicitSchema[F]}, ${inferImplicitSchema[G]}, ${inferImplicitSchema[H]}, ${inferImplicitSchema[R]})"
      )
    )

    val sq = Query8[A, B, C, D, E, F, G, H, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag),
      tupleTag(c)(fTag),
      tupleTag(c)(gTag),
      tupleTag(c)(hTag)
    )
    QueriesGen
      .typecheck(sq)(
        schemas._1,
        schemas._2,
        schemas._3,
        schemas._4,
        schemas._5,
        schemas._6,
        schemas._7,
        schemas._8,
        schemas._9
      )
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query8[A, B, C, D, E, F, G, H, R]](
            q"_root_.com.spotify.scio.sql.Query8($query, $aTag, $bTag, $cTag, $dTag, $eTag, $fTag, $gTag, $hTag)"
          )
      )
  }

  def typed9Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    F: c.WeakTypeTag,
    G: c.WeakTypeTag,
    H: c.WeakTypeTag,
    I: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]],
    fTag: c.Expr[TupleTag[F]],
    gTag: c.Expr[TupleTag[G]],
    hTag: c.Expr[TupleTag[H]],
    iTag: c.Expr[TupleTag[I]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    fSchema: c.Expr[Schema[F]],
    gSchema: c.Expr[Schema[G]],
    hSchema: c.Expr[Schema[H]],
    iSchema: c.Expr[Schema[I]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query9[A, B, C, D, E, F, G, H, I, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[F](c)
    assertConcrete[G](c)
    assertConcrete[H](c)
    assertConcrete[I](c)
    assertConcrete[R](c)

    val schemas: (
      Schema[A],
      Schema[B],
      Schema[C],
      Schema[D],
      Schema[E],
      Schema[F],
      Schema[G],
      Schema[H],
      Schema[I],
      Schema[R]
    ) = c.eval(
      c.Expr(
        q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[
          D
        ]}, ${inferImplicitSchema[E]}, ${inferImplicitSchema[F]}, ${inferImplicitSchema[G]}, ${inferImplicitSchema[
          H
        ]}, ${inferImplicitSchema[I]}, ${inferImplicitSchema[R]})"
      )
    )

    val sq = Query9[A, B, C, D, E, F, G, H, I, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag),
      tupleTag(c)(fTag),
      tupleTag(c)(gTag),
      tupleTag(c)(hTag),
      tupleTag(c)(iTag)
    )
    QueriesGen
      .typecheck(sq)(
        schemas._1,
        schemas._2,
        schemas._3,
        schemas._4,
        schemas._5,
        schemas._6,
        schemas._7,
        schemas._8,
        schemas._9,
        schemas._10
      )
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query9[A, B, C, D, E, F, G, H, I, R]](
            q"_root_.com.spotify.scio.sql.Query9($query, $aTag, $bTag, $cTag, $dTag, $eTag, $fTag, $gTag, $hTag, $iTag)"
          )
      )
  }

  def typed10Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    F: c.WeakTypeTag,
    G: c.WeakTypeTag,
    H: c.WeakTypeTag,
    I: c.WeakTypeTag,
    J: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]],
    fTag: c.Expr[TupleTag[F]],
    gTag: c.Expr[TupleTag[G]],
    hTag: c.Expr[TupleTag[H]],
    iTag: c.Expr[TupleTag[I]],
    jTag: c.Expr[TupleTag[J]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    fSchema: c.Expr[Schema[F]],
    gSchema: c.Expr[Schema[G]],
    hSchema: c.Expr[Schema[H]],
    iSchema: c.Expr[Schema[I]],
    jSchema: c.Expr[Schema[J]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query10[A, B, C, D, E, F, G, H, I, J, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[F](c)
    assertConcrete[G](c)
    assertConcrete[H](c)
    assertConcrete[I](c)
    assertConcrete[J](c)
    assertConcrete[R](c)

    val schemas: (
      Schema[A],
      Schema[B],
      Schema[C],
      Schema[D],
      Schema[E],
      Schema[F],
      Schema[G],
      Schema[H],
      Schema[I],
      Schema[J],
      Schema[R]
    ) = c.eval(
      c.Expr(
        q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[C]}, ${inferImplicitSchema[
          D
        ]}, ${inferImplicitSchema[E]}, ${inferImplicitSchema[F]}, ${inferImplicitSchema[G]}, ${inferImplicitSchema[
          H
        ]}, ${inferImplicitSchema[I]}, ${inferImplicitSchema[J]}, ${inferImplicitSchema[R]})"
      )
    )

    val sq = Query10[A, B, C, D, E, F, G, H, I, J, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag),
      tupleTag(c)(fTag),
      tupleTag(c)(gTag),
      tupleTag(c)(hTag),
      tupleTag(c)(iTag),
      tupleTag(c)(jTag)
    )
    QueriesGen
      .typecheck(sq)(
        schemas._1,
        schemas._2,
        schemas._3,
        schemas._4,
        schemas._5,
        schemas._6,
        schemas._7,
        schemas._8,
        schemas._9,
        schemas._10,
        schemas._11
      )
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query10[A, B, C, D, E, F, G, H, I, J, R]](
            q"_root_.com.spotify.scio.sql.Query10($query, $aTag, $bTag, $cTag, $dTag, $eTag, $fTag, $gTag, $hTag, $iTag, $jTag)"
          )
      )
  }
}
object QueryMacrosGen extends QueryMacrosGen
