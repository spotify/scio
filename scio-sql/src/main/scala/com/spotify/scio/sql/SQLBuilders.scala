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

import com.spotify.scio.schemas.Schema
import org.apache.beam.sdk.values.TupleTag
import scala.reflect._

object SQLBuilders {

  private[sql] def from[A: ClassTag](
    q: String,
    refA: SCollectionRef[A],
    aTag: TupleTag[A],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(refA.coll)(refA.schema, classTag[A])
        .queryAs(new Query1[refA._A, R](q, aTag, udfs))
  }

  private[sql] def from[A: ClassTag, B: ClassTag](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(refA.coll, refB.coll)(refA.schema, classTag[A], refB.schema, classTag[B])
        .queryAs(new Query2[refA._A, refB._A, R](q, aTag, bTag, udfs))
  }

  private[sql] def from[A: ClassTag, B: ClassTag, C: ClassTag](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(refA.coll, refB.coll, refC.coll)(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C]
        )
        .queryAs(new Query3[refA._A, refB._A, refC._A, R](q, aTag, bTag, cTag, udfs))
  }

  private[sql] def from[A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    refD: SCollectionRef[D],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(refA.coll, refB.coll, refC.coll, refD.coll)(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C],
          refD.schema,
          classTag[D]
        )
        .queryAs(new Query4[refA._A, refB._A, refC._A, refD._A, R](q, aTag, bTag, cTag, dTag, udfs))
  }

  private[sql] def from[A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, E: ClassTag](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    refD: SCollectionRef[D],
    refE: SCollectionRef[E],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(refA.coll, refB.coll, refC.coll, refD.coll, refE.coll)(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C],
          refD.schema,
          classTag[D],
          refE.schema,
          classTag[E]
        )
        .queryAs(
          new Query5[refA._A, refB._A, refC._A, refD._A, refE._A, R](
            q,
            aTag,
            bTag,
            cTag,
            dTag,
            eTag,
            udfs
          )
        )
  }

  private[sql] def from[
    A: ClassTag,
    B: ClassTag,
    C: ClassTag,
    D: ClassTag,
    E: ClassTag,
    F: ClassTag
  ](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    refD: SCollectionRef[D],
    refE: SCollectionRef[E],
    refF: SCollectionRef[F],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(refA.coll, refB.coll, refC.coll, refD.coll, refE.coll, refF.coll)(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C],
          refD.schema,
          classTag[D],
          refE.schema,
          classTag[E],
          refF.schema,
          classTag[F]
        )
        .queryAs(
          new Query6[refA._A, refB._A, refC._A, refD._A, refE._A, refF._A, R](
            q,
            aTag,
            bTag,
            cTag,
            dTag,
            eTag,
            fTag,
            udfs
          )
        )
  }

  private[sql] def from[
    A: ClassTag,
    B: ClassTag,
    C: ClassTag,
    D: ClassTag,
    E: ClassTag,
    F: ClassTag,
    G: ClassTag
  ](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    refD: SCollectionRef[D],
    refE: SCollectionRef[E],
    refF: SCollectionRef[F],
    refG: SCollectionRef[G],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(refA.coll, refB.coll, refC.coll, refD.coll, refE.coll, refF.coll, refG.coll)(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C],
          refD.schema,
          classTag[D],
          refE.schema,
          classTag[E],
          refF.schema,
          classTag[F],
          refG.schema,
          classTag[G]
        )
        .queryAs(
          new Query7[refA._A, refB._A, refC._A, refD._A, refE._A, refF._A, refG._A, R](
            q,
            aTag,
            bTag,
            cTag,
            dTag,
            eTag,
            fTag,
            gTag,
            udfs
          )
        )
  }

  private[sql] def from[
    A: ClassTag,
    B: ClassTag,
    C: ClassTag,
    D: ClassTag,
    E: ClassTag,
    F: ClassTag,
    G: ClassTag,
    H: ClassTag
  ](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    refD: SCollectionRef[D],
    refE: SCollectionRef[E],
    refF: SCollectionRef[F],
    refG: SCollectionRef[G],
    refH: SCollectionRef[H],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(
          refA.coll,
          refB.coll,
          refC.coll,
          refD.coll,
          refE.coll,
          refF.coll,
          refG.coll,
          refH.coll
        )(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C],
          refD.schema,
          classTag[D],
          refE.schema,
          classTag[E],
          refF.schema,
          classTag[F],
          refG.schema,
          classTag[G],
          refH.schema,
          classTag[H]
        )
        .queryAs(
          new Query8[refA._A, refB._A, refC._A, refD._A, refE._A, refF._A, refG._A, refH._A, R](
            q,
            aTag,
            bTag,
            cTag,
            dTag,
            eTag,
            fTag,
            gTag,
            hTag,
            udfs
          )
        )
  }

  private[sql] def from[
    A: ClassTag,
    B: ClassTag,
    C: ClassTag,
    D: ClassTag,
    E: ClassTag,
    F: ClassTag,
    G: ClassTag,
    H: ClassTag,
    I: ClassTag
  ](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    refD: SCollectionRef[D],
    refE: SCollectionRef[E],
    refF: SCollectionRef[F],
    refG: SCollectionRef[G],
    refH: SCollectionRef[H],
    refI: SCollectionRef[I],
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(
          refA.coll,
          refB.coll,
          refC.coll,
          refD.coll,
          refE.coll,
          refF.coll,
          refG.coll,
          refH.coll,
          refI.coll
        )(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C],
          refD.schema,
          classTag[D],
          refE.schema,
          classTag[E],
          refF.schema,
          classTag[F],
          refG.schema,
          classTag[G],
          refH.schema,
          classTag[H],
          refI.schema,
          classTag[I]
        )
        .queryAs(
          new Query9[
            refA._A,
            refB._A,
            refC._A,
            refD._A,
            refE._A,
            refF._A,
            refG._A,
            refH._A,
            refI._A,
            R
          ](q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, udfs)
        )
  }

  private[sql] def from[
    A: ClassTag,
    B: ClassTag,
    C: ClassTag,
    D: ClassTag,
    E: ClassTag,
    F: ClassTag,
    G: ClassTag,
    H: ClassTag,
    I: ClassTag,
    J: ClassTag
  ](
    q: String,
    refA: SCollectionRef[A],
    refB: SCollectionRef[B],
    refC: SCollectionRef[C],
    refD: SCollectionRef[D],
    refE: SCollectionRef[E],
    refF: SCollectionRef[F],
    refG: SCollectionRef[G],
    refH: SCollectionRef[H],
    refI: SCollectionRef[I],
    refJ: SCollectionRef[J],
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
    udfs: List[Udf]
  ): SQLBuilder = new SQLBuilder {
    def as[R: Schema: ClassTag] =
      Sql
        .from(
          refA.coll,
          refB.coll,
          refC.coll,
          refD.coll,
          refE.coll,
          refF.coll,
          refG.coll,
          refH.coll,
          refI.coll,
          refJ.coll
        )(
          refA.schema,
          classTag[A],
          refB.schema,
          classTag[B],
          refC.schema,
          classTag[C],
          refD.schema,
          classTag[D],
          refE.schema,
          classTag[E],
          refF.schema,
          classTag[F],
          refG.schema,
          classTag[G],
          refH.schema,
          classTag[H],
          refI.schema,
          classTag[I],
          refJ.schema,
          classTag[J]
        )
        .queryAs(
          new Query10[
            refA._A,
            refB._A,
            refC._A,
            refD._A,
            refE._A,
            refF._A,
            refG._A,
            refH._A,
            refI._A,
            refJ._A,
            R
          ](q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, jTag, udfs)
        )
  }

  private[sql] def from(
    q: String,
    l: List[(SCollectionRef[Any], TupleTag[Any])],
    udfs: List[Udf]
  ): SQLBuilder =
    l match {

      case (refA, aTag) :: Nil =>
        from[refA._A](q, refA, aTag, udfs)

      case (refA, aTag) :: (refB, bTag) :: Nil =>
        from[refA._A, refB._A](q, refA, refB, aTag, bTag, udfs)

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: Nil =>
        from[refA._A, refB._A, refC._A](q, refA, refB, refC, aTag, bTag, cTag, udfs)

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: (refD, dTag) :: Nil =>
        from[refA._A, refB._A, refC._A, refD._A](
          q,
          refA,
          refB,
          refC,
          refD,
          aTag,
          bTag,
          cTag,
          dTag,
          udfs
        )

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: (refD, dTag) :: (refE, eTag) :: Nil =>
        from[refA._A, refB._A, refC._A, refD._A, refE._A](
          q,
          refA,
          refB,
          refC,
          refD,
          refE,
          aTag,
          bTag,
          cTag,
          dTag,
          eTag,
          udfs
        )

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: (refD, dTag) :: (refE, eTag) :: (
            refF,
            fTag
          ) :: Nil =>
        from[refA._A, refB._A, refC._A, refD._A, refE._A, refF._A](
          q,
          refA,
          refB,
          refC,
          refD,
          refE,
          refF,
          aTag,
          bTag,
          cTag,
          dTag,
          eTag,
          fTag,
          udfs
        )

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: (refD, dTag) :: (refE, eTag) :: (
            refF,
            fTag
          ) :: (refG, gTag) :: Nil =>
        from[refA._A, refB._A, refC._A, refD._A, refE._A, refF._A, refG._A](
          q,
          refA,
          refB,
          refC,
          refD,
          refE,
          refF,
          refG,
          aTag,
          bTag,
          cTag,
          dTag,
          eTag,
          fTag,
          gTag,
          udfs
        )

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: (refD, dTag) :: (refE, eTag) :: (
            refF,
            fTag
          ) :: (refG, gTag) :: (refH, hTag) :: Nil =>
        from[refA._A, refB._A, refC._A, refD._A, refE._A, refF._A, refG._A, refH._A](
          q,
          refA,
          refB,
          refC,
          refD,
          refE,
          refF,
          refG,
          refH,
          aTag,
          bTag,
          cTag,
          dTag,
          eTag,
          fTag,
          gTag,
          hTag,
          udfs
        )

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: (refD, dTag) :: (refE, eTag) :: (
            refF,
            fTag
          ) :: (refG, gTag) :: (refH, hTag) :: (refI, iTag) :: Nil =>
        from[refA._A, refB._A, refC._A, refD._A, refE._A, refF._A, refG._A, refH._A, refI._A](
          q,
          refA,
          refB,
          refC,
          refD,
          refE,
          refF,
          refG,
          refH,
          refI,
          aTag,
          bTag,
          cTag,
          dTag,
          eTag,
          fTag,
          gTag,
          hTag,
          iTag,
          udfs
        )

      case (refA, aTag) :: (refB, bTag) :: (refC, cTag) :: (refD, dTag) :: (refE, eTag) :: (
            refF,
            fTag
          ) :: (refG, gTag) :: (refH, hTag) :: (refI, iTag) :: (refJ, jTag) :: Nil =>
        from[
          refA._A,
          refB._A,
          refC._A,
          refD._A,
          refE._A,
          refF._A,
          refG._A,
          refH._A,
          refI._A,
          refJ._A
        ](
          q,
          refA,
          refB,
          refC,
          refD,
          refE,
          refF,
          refG,
          refH,
          refI,
          refJ,
          aTag,
          bTag,
          cTag,
          dTag,
          eTag,
          fTag,
          gTag,
          hTag,
          iTag,
          jTag,
          udfs
        )

      case ts =>
        throw new IllegalArgumentException(
          "sql interpolation only support JOIN on up to 10 unique " +
            s"SCollections, found ${ts.length}"
        )
    }
}
