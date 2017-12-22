/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.tensorflow

import com.google.protobuf.ByteString
import com.spotify.scio.tensorflow.FeatureKind.FeatureKind

import scala.reflect.runtime.universe._

object FeatureKind extends Enumeration {
  type FeatureKind = Value
  val BytesList, FloatList, Int64List = Value

  // scalastyle:off cyclomatic.complexity
  def apply(t: Type): FeatureKind = t match {
    case t if t.erasure =:= typeOf[Option[_]].erasure => this (t.typeArgs.head)
    case t if t.erasure <:< typeOf[Traversable[_]].erasure ||
      t.erasure <:< typeOf[Array[_]] => this (t.typeArgs.head)
    case t if t =:= typeOf[Boolean] => FeatureKind.Int64List
    case t if t =:= typeOf[Long] => FeatureKind.Int64List
    case t if t =:= typeOf[Int] => FeatureKind.Int64List
    case t if t =:= typeOf[Float] => FeatureKind.FloatList
    case t if t =:= typeOf[Double] => FeatureKind.FloatList
    case t if t =:= typeOf[String] => FeatureKind.BytesList
    case t if t =:= typeOf[ByteString] => FeatureKind.BytesList
    case t if t =:= typeOf[Byte] => FeatureKind.BytesList
  }

  // scalastyle:on cyclomatic.complexity
}
