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

package com.spotify.scio.avro.types

import scala.reflect.macros._
import scala.reflect.runtime.universe._

private[types] object MacroUtil {
  // Case class helpers for runtime reflection

  def isCaseClass(t: Type): Boolean =
    !t.toString.startsWith("scala.") &&
      List(typeOf[Product], typeOf[Serializable], typeOf[Equals])
        .forall(b => t.baseClasses.contains(b.typeSymbol))

  def isField(s: Symbol): Boolean =
    s.isPublic && s.isMethod && !s.isSynthetic && !s.isConstructor

  // Case class helpers for macros

  def isCaseClass(c: blackbox.Context)(t: c.Type): Boolean = {
    import c.universe._
    !t.toString.startsWith("scala.") &&
    List(typeOf[Product], typeOf[Serializable], typeOf[Equals])
      .forall(b => t.baseClasses.contains(b.typeSymbol))
  }

  def isField(c: blackbox.Context)(s: c.Symbol): Boolean =
    s.isPublic && s.isMethod && !s.isSynthetic && !s.isConstructor
  def getFields(c: blackbox.Context)(t: c.Type): Iterable[c.Symbol] =
    t.decls.filter(isField(c))

  // Namespace helpers

  val ScioAvro = "_root_.com.spotify.scio.avro"
  val ApacheAvro = "_root_.org.apache.avro"
  val ScioAvroType: String = s"$ScioAvro.types.AvroType"

  def p(c: blackbox.Context, code: String): c.Tree = c.parse(code)
}
