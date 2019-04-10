/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio

import com.spotify.scio.values.SCollection
import com.spotify.scio.schemas.Schema
import org.apache.beam.sdk.values.TupleTag

import scala.language.implicitConversions

package object sql {

  sealed trait SQLBuilder {
    def as[B: Schema]: SCollection[B]
  }

  sealed trait SqlParam
  final case class SCollectionRef[A: Schema](coll: SCollection[A]) extends SqlParam {
    type _A = A
    val schema = Schema[A]
  }
  final case class UdfRef(udf: Udf) extends SqlParam

  implicit def toUdfRef(udf: Udf): SqlParam = new UdfRef(udf)
  implicit def toSCollectionRef[A: Schema](coll: SCollection[A]): SqlParam =
    new SCollectionRef[A](coll)

  final implicit class SqlInterpolator(val sc: StringContext) extends AnyVal {

    // TODO: typed SQL ?
    // TODO: at least 1 params is a SCollectionRef
    def sql(p0: SqlParam, ps: SqlParam*): SQLBuilder = {
      val params = p0 :: ps.toList
      val tags =
        params.zipWithIndex.collect {
          case (ref @ SCollectionRef(scoll), i) =>
            (scoll.name, (ref, new TupleTag[ref._A](s"SCOLLECTION_$i")))
        }.toMap

      val udfs =
        params.collect {
          case UdfRef(u) => u
        }

      val strings = sc.parts.iterator
      val expressions = params.iterator
      var buf = new StringBuffer(strings.next)
      while (strings.hasNext) {
        val param = expressions.next
        val p =
          param match {
            case SCollectionRef(scoll) =>
              tags(scoll.name)._2.getId
            case UdfRef(udf) =>
              udf.fnName
          }
        buf.append(p)
        buf.append(strings.next)
      }
      val q = buf.toString

      tags.toList match {
        case (name, (ref, tag)) :: Nil =>
          new SQLBuilder {
            def as[B: Schema] =
              Sql
                .from(ref.coll)(ref.schema)
                .queryAs(new Query[ref._A, B](q, tag, udfs))
          }
        case (name0, (ref0, tag0)) :: (name1, (ref1, tag1)) :: Nil =>
          new SQLBuilder {
            def as[B: Schema] =
              Sql
                .from(ref0.coll, ref1.coll)(ref0.schema, ref1.schema)
                .queryAs(new Query2[ref0._A, ref1._A, B](q, tag0, tag1, udfs))
          }
        case _ =>
          throw new IllegalArgumentException("WUUUUTTT????")
      }
    }

    // def sql[A: Schema](coll: SCollection[A]): SQLBuilder = {
    //   val strings = sc.parts.iterator
    //   val name = Sql.SCollectionTypeName
    //   var buf = new StringBuffer(strings.next)
    //   while (strings.hasNext) {
    //     buf.append(name)
    //     buf.append(strings.next)
    //   }

    //   new SQLBuilder {
    //     val q = buf.toString
    //     val tag = new TupleTag[A](name)
    //     def as[B: Schema] = Sql.from(coll).queryAs(new Query[A, B](q, tag))
    //   }
    // }
  }

}
