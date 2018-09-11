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

package com.spotify.scio.bigquery.types

import org.slf4j.LoggerFactory

import scala.reflect.macros._
import scala.reflect.runtime.universe._

private[types] object MacroUtil {

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  // Case class helpers for runtime reflection

  def isCaseClass(t: Type): Boolean =
    !t.toString.startsWith("scala.") &&
      List(typeOf[Product], typeOf[Serializable], typeOf[Equals])
        .forall(b => t.baseClasses.contains(b.typeSymbol))

  def isField(s: Symbol): Boolean = s.isPublic && s.isMethod && !s.isSynthetic && !s.isConstructor

  // Case class helpers for macros

  def isCaseClass(c: blackbox.Context)(t: c.Type): Boolean = {
    import c.universe._
    !t.toString.startsWith("scala.") &&
      List(typeOf[Product], typeOf[Serializable], typeOf[Equals])
        .forall(b => t.baseClasses.contains(b.typeSymbol))
  }

  def isField(c: blackbox.Context)(s: c.Symbol): Boolean =
    s.isPublic && s.isMethod && !s.isSynthetic && !s.isConstructor
  def getFields(c: blackbox.Context)(t: c.Type): Iterable[c.Symbol] = {
    import c.universe._
    val fields = t.decls.filter(isField(c))
    // if type was macro generated it should have bigquery tag on it
    if (t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[BigQueryTag])) {
      fields
        .filter { s =>
          try {
            val AnnotatedType(a, _) = s.asMethod.returnType
            a.exists(_.tree.tpe == typeOf[BigQueryTag])
          } catch {
            case _: MatchError => false
          }
        }
    } else {
      fields
    }
  }

  // Debugging

  @inline def debug(msg: Any): Unit = {
    if (sys.props("bigquery.types.debug") != null && sys.props("bigquery.types.debug").toBoolean) {
      logger.info(msg.toString)
    }
  }

  // Namespace helpers

  val SBQ = "_root_.com.spotify.scio.bigquery"
  val GModel = "_root_.com.google.api.services.bigquery.model"
  val GBQIO = "_root_.org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO"
  val SType = s"$SBQ.types.BigQueryType"
  val SUtil = s"$SBQ.BigQueryUtil"

  def p(c: blackbox.Context, code: String): c.Tree = c.parse(code)

}
