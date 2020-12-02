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
import com.spotify.scio.macros.DerivationUtils

import scala.deriving._
import scala.quoted._
import scala.compiletime._

trait LowPrioritySchemaDerivation {
  type Typeclass[T] = Schema[T]

  // back compat
  inline def gen[T <: Product](implicit m: Mirror.Of[T]): Schema[T] =
    derived[T]

  def coderProduct[T <: Product](p: Mirror.ProductOf[T], schemas: List[(String, Schema[Any])]): Schema[T] = {
    val rawConstruct:  Seq[Any] => T =
      vs => p.fromProduct(Tuple.fromArray(vs.toArray))

    val destruct: T => Array[Any] =
      t => Tuple.fromProduct(t).toArray.asInstanceOf[Array[Any]]

    Record(schemas.toArray, rawConstruct, destruct)
  }

  inline implicit def derived[T <: Product](implicit m: Mirror.Of[T]): Schema[T] = {
    inline m match {
      case p: Mirror.ProductOf[T] =>
        val elemInstances =
          DerivationUtils.summonAllF[Schema, p.MirroredElemTypes]
            .toList
            .asInstanceOf[List[Schema[Any]]] // YOLO
        val fields = DerivationUtils.mirrorFields[p.MirroredElemLabels]
        val schemas = fields.zip(elemInstances)
        coderProduct(p, schemas)
    }
  }
}
