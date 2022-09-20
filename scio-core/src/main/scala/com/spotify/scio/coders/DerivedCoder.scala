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

import scala.reflect.ClassTag

object LowPriorityCoderDerivation {

  private object ProductIndexedSeqLike {
    def apply(p: Product): ProductIndexedSeqLike = new ProductIndexedSeqLike(p)
  }

  // Instead of converting Product.productIterator to a Seq, create a wrapped around it
  final private class ProductIndexedSeqLike private (val p: Product) extends IndexedSeq[Any] {
    override def length: Int = p.productArity
    override def apply(i: Int): Any = p.productElement(i)
  }

}

trait LowPriorityCoderDerivation {
  import LowPriorityCoderDerivation._
  import magnolia1._

  type Typeclass[T] = Coder[T]

  def join[T: ClassTag](ctx: CaseClass[Coder, T]): Coder[T] = {
    val typeName = ctx.typeName.full
    // calling patched rawConstruct on empty object should work
    val emptyCtx = ClosureCleaner
      .instantiateClass(ctx.getClass)
      .asInstanceOf[CaseClass[Coder, T]]
    if (ctx.isValueClass) {
      val p = ctx.parameters.head
      Coder.xmap(p.typeclass.asInstanceOf[Coder[Any]])(
        v => emptyCtx.rawConstruct(Seq(v)),
        p.dereference
      )
    } else if (ctx.isObject) {
      Coder.singleton(typeName, () => emptyCtx.rawConstruct(Seq.empty))
    } else {
      Coder.ref(typeName) {
        val cs = Array.ofDim[(String, Coder[Any])](ctx.parameters.length)
        ctx.parameters.foreach { p =>
          cs.update(p.index, p.label -> p.typeclass.asInstanceOf[Coder[Any]])
        }

        Coder.record[T](typeName, cs)(
          emptyCtx.rawConstruct,
          v => ProductIndexedSeqLike(v.asInstanceOf[Product])
        )
      }
    }
  }

  def split[T](sealedTrait: SealedTrait[Coder, T]): Coder[T] = {
    val typeName = sealedTrait.typeName.full
    val coders = sealedTrait.subtypes
      .map(s => (s.index, s.typeclass.asInstanceOf[Coder[T]]))
      .toMap

    val subtypes = sealedTrait.subtypes
      .map { s =>
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

        clean
      }

    val id = (v: T) => subtypes.find(_.cast.isDefinedAt(v)).map(_.index).get
    if (sealedTrait.subtypes.length <= 2) {
      val booleanId: Int => Boolean = _ != 0
      val cs = coders.map { case (key, v) => (booleanId(key), v) }
      Coder.disjunction[T, Boolean](typeName, cs)(id.andThen(booleanId))
    } else {
      Coder.disjunction[T, Int](typeName, coders)(id)
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
