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

import scala.deriving._
import scala.compiletime._
import scala.quoted._
import com.spotify.scio.macros.DerivationUtils

private object Derived extends Serializable {
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

  def coderProduct[T](
    p: Mirror.ProductOf[T],
    typeName: String,
    cs: Array[(String, Coder[Any])]
  ): Coder[T] = {
    val rawConstruct:  Seq[Any] => T =
      vs => p.fromProduct(Tuple.fromArray(vs.toArray))

    // TODO: scala3 - check stack trace correctness
    val materializationStack = CoderStackTrace.prepare

    val constructor: Seq[Any] => T =
      ps =>
        catching(s"Error while constructing object from parameters $ps", materializationStack)(
          rawConstruct(ps)
        )

    // TODO: error handling ? (can it even fail ?)
    val destruct: T => Array[Any] =
      // XXX: scala3 - I should probably not need to cast T to Product
      t => Tuple.fromProduct(t.asInstanceOf[Product]).toArray.asInstanceOf[Array[Any]]

    Coder.record[T](typeName, cs, constructor, destruct)
  }

  def coderSum[T](
    s: Mirror.SumOf[T],
    typeName: String,
    coders: Map[Int, Coder[T]]
  ): Coder[T] = {
    if(coders.size <= 2) { // trait has 2 or less implementations
      val booleanId: Int => Boolean = _ != 0
      val cs = coders.map { case (key, v) => (booleanId(key), v) }
      Coder.disjunction[T, Boolean](typeName, cs) { t =>
        booleanId(s.ordinal(t))
      }
    } else {
      Coder.disjunction[T, Int](typeName, coders) { t =>
        s.ordinal(t)
      }
    }
  }
}

trait LowPriorityCoderDerivation {
  import Derived._

  type Typeclass[T] = Coder[T]

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
  inline def gen[T](implicit m: Mirror.Of[T]): Coder[T] =
    derived[T]

  inline implicit def derived[T](implicit m: Mirror.Of[T]): Coder[T] = {
    inline m match {
      case p: Mirror.ProductOf[T] =>
        val instances = DerivationUtils.summonAllF[Coder, p.MirroredElemTypes].toList.asInstanceOf[List[Coder[Any]]]
        val typeName: String = constValue[p.MirroredLabel] // XXX: scala3 - How do I get the FULL class name
        val fields = DerivationUtils.mirrorFields[p.MirroredElemLabels]
        val cs = fields.zip(instances).toArray
        coderProduct[T](p, typeName, cs)
      case s: Mirror.SumOf[T] =>
        val instances = DerivationUtils.summonAllF[Coder, s.MirroredElemTypes].toList
        val typeName: String = constValue[s.MirroredLabel] // XXX: scala3 - How do I get the FULL class name
        val coders: Map[Int, Coder[T]] = instances.asInstanceOf[List[Coder[T]]].zipWithIndex.map((v, i) => (i, v)).toMap
        coderSum[T](s, typeName, coders)
    }
  }
}
