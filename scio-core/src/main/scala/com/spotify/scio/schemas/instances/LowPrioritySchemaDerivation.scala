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

package com.spotify.scio.schemas.instances

import com.spotify.scio.schemas._

private object Derived extends Serializable {
  import magnolia._
  def combineSchema[T](ps: Seq[Param[Schema, T]], rawConstruct: Seq[Any] => T): Record[T] = {
    @inline def destruct(v: T): Array[Any] = {
      val arr = new Array[Any](ps.length)
      var i = 0
      while (i < ps.length) {
        val p = ps(i)
        arr.update(i, p.dereference(v))
        i = i + 1
      }
      arr
    }
    val schemas = ps.iterator.map(p => p.label -> p.typeclass.asInstanceOf[Schema[Any]]).toArray

    Record(schemas, rawConstruct, destruct)
  }
}

trait LowPrioritySchemaDerivation {
  import magnolia._
  type Typeclass[T] = Schema[T]

  def combine[T](ctx: CaseClass[Schema, T]): Record[T] =
    Derived.combineSchema(ctx.parameters, ctx.rawConstruct)

  import com.spotify.scio.MagnoliaMacros
  implicit def gen[T]: Schema[T] = macro MagnoliaMacros.genWithoutAnnotations[T]
}
