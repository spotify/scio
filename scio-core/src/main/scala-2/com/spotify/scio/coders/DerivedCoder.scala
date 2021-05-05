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

private object Derived extends Serializable {
  import magnolia._

  @inline private def catching[T](msg: => String, stack: Array[StackTraceElement])(v: => T): T =
    try {
      v
    } catch {
      case e: Exception =>
        /* prior to scio 0.8, a wrapped exception was thrown. It is no longer the case, as some
        backends (e.g. Flink) use exceptions as a way to signal from the Coder to the layers above
         here; we therefore must alter the type of exceptions passing through this block.
         */
        throw CoderStackTrace.append(e, Some(msg), stack)
    }

  def combineCoder[T](
    typeName: TypeName,
    ps: Seq[Param[Coder, T]],
    rawConstruct: Seq[Any] => T
  ): Coder[T] =
    Ref(
      typeName.full, {
        val cs = new Array[(String, Coder[Any])](ps.length)
        var i = 0
        while (i < ps.length) {
          val p = ps(i)
          cs.update(i, (p.label, p.typeclass.asInstanceOf[Coder[Any]]))
          i = i + 1
        }

        val materializationStack = CoderStackTrace.prepare

        @inline def destruct(v: T): Array[Any] = {
          val arr = new Array[Any](ps.length)
          var i = 0
          while (i < ps.length) {
            val p = ps(i)
            catching(
              s"Error while dereferencing parameter ${p.label} in $v",
              materializationStack
            ) {
              arr.update(i, p.dereference(v))
              i = i + 1
            }
          }
          arr
        }

        val constructor: Seq[Any] => T =
          ps =>
            catching(s"Error while constructing object from parameters $ps", materializationStack)(
              rawConstruct(ps)
            )

        Coder.record[T](typeName.full, cs, constructor, destruct)
      }
    )
}

trait LowPriorityCoderDerivation {
  import magnolia._

  type Typeclass[T] = Coder[T]

  def combine[T](ctx: CaseClass[Coder, T]): Coder[T] =
    if (ctx.isValueClass) {
      Coder.xmap(ctx.parameters(0).typeclass.asInstanceOf[Coder[Any]])(
        a => ctx.rawConstruct(Seq(a)),
        ctx.parameters(0).dereference
      )
    } else {
      Derived.combineCoder(ctx.typeName, ctx.parameters, ctx.rawConstruct)
    }

  def dispatch[T](sealedTrait: SealedTrait[Coder, T]): Coder[T] = {
    val typeName = sealedTrait.typeName.full
    val idx: Map[magnolia.TypeName, Int] =
      sealedTrait.subtypes.map(_.typeName).zipWithIndex.toMap
    val coders: Map[Int, Coder[T]] =
      sealedTrait.subtypes
        .map(_.typeclass.asInstanceOf[Coder[T]])
        .zipWithIndex
        .map { case (c, i) => (i, c) }
        .toMap

    if (sealedTrait.subtypes.length <= 2) {
      val booleanId: Int => Boolean = _ != 0
      val cs = coders.map { case (key, v) => (booleanId(key), v) }
      Coder.disjunction[T, Boolean](typeName, cs) { t =>
        sealedTrait.dispatch(t)(subtype => booleanId(idx(subtype.typeName)))
      }
    } else {
      Coder.disjunction[T, Int](typeName, coders) { t =>
        sealedTrait.dispatch(t)(subtype => idx(subtype.typeName))
      }
    }
  }

  /**
   * Derive a Coder for a type T given implicit coders of all parameters in the constructor
   * of type T is in scope. For sealed trait, implicit coders of parameters of the constructors
   * of all sub-types should be in scope.
   *
   * In case of a missing [[shapeless.LowPriority]] implicit error when calling this method as
   * [[Coder.gen[Type] ]], it means that Scio is unable to derive a BeamCoder for some parameter
   * [P] in the constructor of Type. This happens when no implicit Coder instance for type P is
   * in scope. This is fixed by placing an implicit Coder of type P in scope, using
   * [[Coder.kryo[P] ]] or defining the Coder manually (see also [[Coder.xmap]])
   */
  implicit def gen[T]: Coder[T] = macro CoderMacros.wrappedCoder[T]
}
