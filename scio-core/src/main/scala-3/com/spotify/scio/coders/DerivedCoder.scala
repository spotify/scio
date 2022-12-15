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
import magnolia1._

import scala.reflect.ClassTag
import scala.deriving.Mirror

object LowPriorityCoderDerivation {

  private object ProductIndexedSeqLike {
    def apply(p: Product): ProductIndexedSeqLike = new ProductIndexedSeqLike(p)
  }

  // Instead of converting Product.productIterator to a Seq, create a wrapped around it
  private class ProductIndexedSeqLike private (private val p: Product) extends IndexedSeq[Any] {
    override def length: Int = p.productArity
    override def apply(i: Int): Any = p.productElement(i)
  }

  private object CaseClassConstructor {

    // to create a case class, we only need to serialize the CaseClass's instance class
    def apply[T](caseClass: CaseClass[Coder, T]): CaseClassConstructor[T] =
      new CaseClassConstructor(caseClass.getClass.getName)
  }

  private class CaseClassConstructor[T] private (private val className: String)
      extends Serializable {
    // We can call rawConstruct on an empty CaseClass instance
    @transient lazy val ctx: CaseClass[Coder, T] = ClosureCleaner
      .instantiateClass(Class.forName(className))
      .asInstanceOf[CaseClass[Coder, T]]

    def rawConstruct(fieldValues: Seq[Any]): T = ctx.rawConstruct(fieldValues)
  }

  private object SealedTraitIdentifier {

    // to find the sub-type id, we only we only need to serialize the isInstanceOf and index
    def apply[T](sealedTrait: SealedTrait[Coder, T]): SealedTraitIdentifier[T] = {
      val subtypes = sealedTrait.subtypes
        .map { s =>
          // defeat closure by accessing underlying definition
          val field = s.getClass.getDeclaredField("isType")
          field.setAccessible(true)
          val isType = field.get(s).asInstanceOf[T => Boolean]
          val index = s.index
          isType -> index
        }
      new SealedTraitIdentifier(subtypes)
    }
  }

  private class SealedTraitIdentifier[T] private (private val subTypes: Seq[(T => Boolean, Int)])
      extends Serializable {
    def id(v: T): Int = subTypes.collectFirst { case (isType, index) if isType(v) => index }.get
  }

  def colsureFunction[E, D, R](enclosed: E)(gen: E => D => R): D => R = gen(enclosed)

  def colsureSupplier[E, R](enclosed: E)(gen: E => R): () => R = () => gen(enclosed)

}

trait LowPriorityCoderDerivation extends Derivation[Coder]{

  import LowPriorityCoderDerivation._

  inline def join[T](ctx: CaseClass[Coder, T]): Coder[T] = {
    val typeName = ctx.typeInfo.full
    val constructor = CaseClassConstructor(ctx)
    if (ctx.isValueClass) {
      val p = ctx.params.head
      Coder.explicitXmap(p.typeclass.asInstanceOf[Coder[Any]])(
        colsureFunction(constructor)(c => v => c.rawConstruct(Seq(v))),
        p.deref
      )(ctx.typeInfo.full)
    } else if (ctx.isObject) {
      Coder.singleton(typeName, colsureSupplier(constructor)(_.rawConstruct(Seq.empty)))
    } else {
      Coder.ref(typeName) {
        val cs = Array.ofDim[(String, Coder[Any])](ctx.params.length)
        ctx.params.foreach { p =>
          cs.update(p.index, p.label -> p.typeclass.asInstanceOf[Coder[Any]])
        }

        Coder.record[T](typeName, cs)(
          colsureFunction(constructor)(_.rawConstruct),
          v => ProductIndexedSeqLike(v.asInstanceOf[Product])
        )
      }
    }
  }

  inline def split[T](sealedTrait: SealedTrait[Coder, T]): Coder[T] = {
    val typeName = sealedTrait.typeInfo.full
    val identifier = SealedTraitIdentifier(sealedTrait)
    val coders = sealedTrait.subtypes
      .map(s => (s.index, s.typeclass.asInstanceOf[Coder[T]]))
      .toMap
    if (sealedTrait.subtypes.length <= 2) {
      val booleanId: Int => Boolean = _ != 0
      val cs = coders.map { case (key, v) => (booleanId(key), v) }
      Coder.disjunction[T, Boolean](typeName, cs)(
        colsureFunction(identifier)(_.id).andThen(booleanId)
      )
    } else {
      Coder.disjunction[T, Int](typeName, coders)(colsureFunction(identifier)(_.id))
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
  transparent inline given gen[T: Mirror.Of]: Coder[T] = derived[T] // ${ CoderMacros.c[T] }
}