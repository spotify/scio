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

import com.spotify.scio.MagnoliaMacros
import com.twitter.chill.ClosureCleaner
import magnolia1._

import scala.reflect.ClassTag

private[coders] object CoderDerivation extends CoderDerivation {

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

  private class CaseClassConstructor[T] private (
    private val className: String
  ) extends Serializable {

    @transient lazy val ctxClass: Class[_] = Class.forName(className)

    @transient lazy val ctx: CaseClass[Coder, T] = {
      ctxClass.getDeclaredFields.find {
        _.getName == ClosureCleaner.OUTER
      } match {
        /* The field "$outer" is added by scala compiler to a case class if it is declared inside
         another class. And the constructor of that compiled class requires outer field to be not
          null.
         If "$outer" is present it's an inner class and this scenario is officially not supported
          by Scio */
        case Some(_) =>
          throw new Throwable(
            s"Found an $$outer field in $ctxClass. Possibly it is an attempt to use inner case " +
              "class in a Scio transformation. Inner case classes are not supported in Scio " +
              "auto-derived macros. Move the case class to the package level or define a custom " +
              "coder."
          )
        /* If "$outer" field is absent then T is not an inner class, we create an empty instance
        of ctx */
        case None =>
          ClosureCleaner.instantiateClass(ctxClass).asInstanceOf[CaseClass[Coder, T]]
      }
    }

    def rawConstruct(fieldValues: Seq[Any]): T = ctx.rawConstruct(fieldValues)
  }

  private object SealedTraitIdentifier {

    // to find the sub-type id, we only we only need to serialize the isInstanceOf and index
    def apply[T](sealedTrait: SealedTrait[Coder, T]): SealedTraitIdentifier[T] = {
      val subtypes = sealedTrait.subtypes
        .map { s =>
          // defeat closure by accessing underlying definition
          val field = s.getClass.getDeclaredField("isType$1")
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

  def closureFunction[E, D, R](enclosed: E)(gen: E => D => R): D => R = gen(enclosed)

  def closureSupplier[E, R](enclosed: E)(gen: E => R): () => R = () => gen(enclosed)

}

trait CoderDerivation extends CoderGrammar {

  import CoderDerivation._

  type Typeclass[T] = Coder[T]

  def join[T: ClassTag](ctx: CaseClass[Coder, T]): Coder[T] = {
    val typeName = ctx.typeName.full
    val constructor = CaseClassConstructor(ctx)

    if (ctx.isValueClass) {
      val p = ctx.parameters.head
      xmap(p.typeclass.asInstanceOf[Coder[Any]])(
        closureFunction(constructor)(c => v => c.rawConstruct(Seq(v))),
        p.dereference
      )
    } else if (ctx.isObject) {
      singleton(typeName, closureSupplier(constructor)(_.rawConstruct(Seq.empty)))
    } else {
      ref(typeName) {
        val cs = Array.ofDim[(String, Coder[Any])](ctx.parameters.length)

        ctx.parameters.foreach { p =>
          cs.update(p.index, p.label -> p.typeclass.asInstanceOf[Coder[Any]])
        }

        record[T](typeName, cs)(
          closureFunction(constructor)(_.rawConstruct),
          v => ProductIndexedSeqLike(v.asInstanceOf[Product])
        )
      }
    }
  }

  def split[T](sealedTrait: SealedTrait[Coder, T]): Coder[T] = {
    val typeName = sealedTrait.typeName.full
    val identifier = SealedTraitIdentifier(sealedTrait)
    val coders = sealedTrait.subtypes
      .map(s => (s.index, s.typeclass.asInstanceOf[Coder[T]]))
      .toMap
    if (sealedTrait.subtypes.length <= 2) {
      val booleanId: Int => Boolean = _ != 0
      val cs = coders.map { case (key, v) => (booleanId(key), v) }
      disjunction[T, Boolean](typeName, cs)(
        closureFunction(identifier)(_.id).andThen(booleanId)
      )
    } else {
      disjunction[T, Int](typeName, coders)(closureFunction(identifier)(_.id))
    }
  }

  /**
   * Derive a Coder for a type T given implicit coders of all parameters in the constructor of type
   * T is in scope. For sealed trait, implicit coders of parameters of the constructors of all
   * sub-types should be in scope.
   */
  def gen[T]: Coder[T] = macro MagnoliaMacros.genWithoutAnnotations[T]
}

@deprecated("Use CoderDerivation instead", "0.14.0")
trait LowPriorityCoderDerivation extends CoderDerivation
