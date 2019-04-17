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

package com.spotify.scio

import scala.language.experimental.macros
import scala.reflect.macros._

/**
 * Proof that a type is implemented in Java
 */
sealed trait IsJavaBean[T]

object IsJavaBean {
  implicit def isJavaBean[T]: IsJavaBean[T] = macro IsJavaBean.isJavaBeanImpl[T]

  def apply[T](implicit i: IsJavaBean[T]): IsJavaBean[T] = i

  private def checkGetterAndSetters(c: blackbox.Context)(t: c.universe.Type): Unit = {
    val getters =
      t.decls.collect {
        case s if s.name.toString.startsWith("get") =>
          (s.name.toString.drop(3), s.asMethod.info.asInstanceOf[c.universe.MethodType])
      }

    val setters =
      t.decls.collect {
        case s if s.name.toString.startsWith("set") =>
          (s.name.toString.drop(3), s.asMethod.info.asInstanceOf[c.universe.MethodType])
      }.toMap

    if (getters.isEmpty) {
      val mess = s"""Class $t is not a Java bean since it does not have any getter"""
      c.abort(c.enclosingPosition, mess)
    }

    getters.foreach {
      case (name, info) =>
        val setter =
          setters
            .get(name)
            .getOrElse {
              val mess =
                s"""JavaBean contained a getter for field $name""" +
                  """ but did not contain a matching setter."""
              c.abort(c.enclosingPosition, mess)
            }

        val resType = info.resultType
        val paramType = setter.params.head.asTerm.info

        if (resType != paramType) {
          val mess =
            s"""JavaBean contained setter for field $name that had a mismatching type.
                |  found:    $paramType
                |  expected: $resType""".stripMargin
          c.abort(c.enclosingPosition, mess)
        }
    }
  }

  def isJavaBeanImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val sym = wtt.typeSymbol
    if (sym.isJava && sym.isClass) {
      checkGetterAndSetters(c)(wtt)
      q"null: _root_.com.spotify.scio.IsJavaBean[$wtt]"
    } else {
      c.abort(c.enclosingPosition,
              s"$wtt is not a Java class. (isJava: ${sym.isJava}, isClass: ${sym.isClass})")
    }
  }
}
