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

package com.spotify.scio.coders

private final object Derived extends Serializable {
  import magnolia._
  import Coder.xmap

  def combineCoder[T](ps: Seq[Param[Coder, T]],
                      rawConstruct: Seq[Any] => T): Coder[T] = {
    val cs = new Array[(String, Coder[Any])](ps.length)
    var i = 0
    while (i < ps.length) {
      val p = ps(i)
      cs.update(i, (p.label, p.typeclass.asInstanceOf[Coder[Any]]))
      i = i + 1
    }

    val coderValues = Coder.sequence(cs)
    @inline def cToArray(v: T): Array[Any] = {
      val arr = new Array[Any](ps.length)
      var i = 0
      while (i < ps.length) {
        val p = ps(i)
        arr.update(i, p.dereference(v))
        i = i + 1
      }
      arr
    }
    xmap(coderValues)(xs => rawConstruct(xs), v => cToArray(v))
  }
}

trait LowPriorityCoderDerivation {
  import language.experimental.macros, magnolia._
  import com.spotify.scio.coders.CoderMacros

  type Typeclass[T] = Coder[T]

  def combine[T](ctx: CaseClass[Coder, T]): Coder[T] =
    Derived.combineCoder(ctx.parameters, ctx.rawConstruct _)

  def dispatch[T](sealedTrait: SealedTrait[Coder, T]): Coder[T] = {
    val idx: Map[magnolia.TypeName, Int] =
      sealedTrait.subtypes.map(_.typeName).zipWithIndex.toMap
    val coders: Map[Int, Coder[T]] =
      sealedTrait.subtypes
        .map(_.typeclass.asInstanceOf[Coder[T]])
        .zipWithIndex
        .map { case (c, i) => (i, c) }
        .toMap

    Coder.disjunction[T, Int](coders) { t =>
      sealedTrait.dispatch(t) { subtype =>
        idx(subtype.typeName)
      }
    }
  }

  implicit def gen[T]: Coder[T] = macro CoderMacros.wrappedCoder[T]
}
