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

package com.spotify.scio.coders

import com.twitter.chill.ClosureCleaner

trait LowPriorityCoderDerivation {
  import magnolia1._

  type Typeclass[T] = Coder[T]

  def join[T](ctx: CaseClass[Coder, T]): Coder[T] = {
    if (ctx.isValueClass) {
      val p = ctx.parameters.head
      Coder.xmap(p.typeclass.asInstanceOf[Coder[Any]])(
        v => ctx.rawConstruct(Seq(v)),
        p.dereference
      )
    } else if (ctx.isObject) {
      Coder.singleton(ctx.rawConstruct(Seq.empty))
    } else {
      Ref(
        ctx.typeName.full, {
          val cs = Array.ofDim[(String, Coder[Any])](ctx.parameters.length)
          ctx.parameters.foreach { p =>
            cs.update(p.index, p.label -> p.typeclass.asInstanceOf[Coder[Any]])
          }

          // calling patched rawConstruct on empty object should work
          val emptyCtx = ClosureCleaner
            .instantiateClass(ctx.getClass)
            .asInstanceOf[CaseClass[Coder, T]]
          val construct = emptyCtx.rawConstruct _
          val destruct = (v: T) => v.asInstanceOf[Product].productIterator
          Coder.record[T](ctx.typeName.full, cs, construct, destruct)
        }
      )
    }
  }

  def split[T](sealedTrait: SealedTrait[Coder, T]): Coder[T] = {
    val typeName = sealedTrait.typeName.full
    val coders = sealedTrait.subtypes
      .map(s => (s.index, s.typeclass.asInstanceOf[Coder[T]]))
      .toMap

    val id = sealedTrait.subtypes
      .map[PartialFunction[T, Int]] { s =>
        val clazz = s.getClass
        val clean = ClosureCleaner
          .instantiateClass(clazz)
          .asInstanceOf[Subtype[Coder, T]]
        // copy required fields only
        val isType = clazz.getDeclaredField("isType$1")
        isType.setAccessible(true)
        isType.set(clean, isType.get(s))

        val index = clazz.getDeclaredField("idx$1")
        index.setAccessible(true)
        index.set(clean, index.get(s))

        { case v: T if clean.cast.isDefinedAt(v) => clean.index }
      }
      .reduce(_ orElse _)

    if (sealedTrait.subtypes.length <= 2) {
      val booleanId: Int => Boolean = _ != 0
      val cs = coders.map { case (key, v) => (booleanId(key), v) }
      Coder.disjunction[T, Boolean](typeName, id.andThen(booleanId), cs)
    } else {
      Coder.disjunction[T, Int](typeName, id, coders)
    }
  }

  /**
   * Derive a Coder for a type T given implicit coders of all parameters in the constructor of type
   * T is in scope. For sealed trait, implicit coders of parameters of the constructors of all
   * sub-types should be in scope.
   *
   * In case of a missing [[shapeless.LowPriority]] implicit error when calling this method as
   * [[Coder.gen[Type]] ], it means that Scio is unable to derive a BeamCoder for some parameter [P]
   * in the constructor of Type. This happens when no implicit Coder instance for type P is in
   * scope. This is fixed by placing an implicit Coder of type P in scope, using [[Coder.kryo[P]] ]
   * or defining the Coder manually (see also [[Coder.xmap]])
   */
  implicit def gen[T]: Coder[T] = macro CoderMacros.wrappedCoder[T]
}
