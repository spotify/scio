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

package com.spotify.scio.sql

import com.spotify.scio.annotations.experimental
import com.spotify.scio.values.SCollection
import com.spotify.scio.schemas.Schema
import org.apache.beam.sdk.values.TupleTag

import scala.reflect.macros.{blackbox, whitebox}

import scala.reflect._

trait SQLBuilder {
  def as[B: Schema: ClassTag]: SCollection[B]
}

sealed trait SqlParam

final case class SCollectionRef[A: Schema](coll: SCollection[A]) extends SqlParam {
  type _A = A
  val schema: Schema[A] = Schema[A]
}

final case class UdfRef(udf: Udf) extends SqlParam

final class SqlInterpolator(private val sc: StringContext) extends TypedSQLInterpolator {
  private def paramToString(
    tags: Map[String, (SCollectionRef[_], TupleTag[_])]
  )(p: SqlParam): String =
    p match {
      case SCollectionRef(scoll) =>
        tags(scoll.name)._2.getId
      case UdfRef(udf) =>
        udf.fnName
    }

  @experimental
  def sql(p0: SqlParam, ps: SqlParam*): SQLBuilder = {
    val params = p0 :: ps.toList

    val tags = params
      .collect { case ref @ SCollectionRef(scoll) => (scoll, ref) }
      .distinct
      .zipWithIndex
      .map { case ((scoll, ref), i) =>
        (scoll.name, (ref, new TupleTag[ref._A](s"SCOLLECTION_$i")))
      }
      .toMap

    val udfs = params.collect { case UdfRef(u) => u }
    val strings = sc.parts.iterator
    val expressions = params.iterator
    val toString = paramToString(tags) _

    val expr = expressions.map(toString)
    val q =
      strings.zipAll(expr, "", "").foldLeft("") { case (a, (x, y)) => s"$a$x $y" }

    SQLBuilders.from(q, tags.values.toList.asInstanceOf[List[(SCollectionRef[Any], TupleTag[Any])]], udfs)
  }
}
