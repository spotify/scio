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

import scala.compiletime._
import scala.deriving._
import scala.quoted._

/** Proof that a type is implemented in Java */
sealed trait IsJavaBean[T]

object IsJavaBean {

  private def checkGetterAndSetters(using q: Quotes)(sym: q.reflect.Symbol): Unit = {
    import q.reflect._
    val methods: List[Symbol] = sym.declaredMethods

    val getters =
      methods.collect {
        case s if s.name.toString.startsWith("get") =>
          (s.name.toString.drop(3), s.tree.asInstanceOf[DefDef])
      }

    val setters =
      methods.collect {
        case s if s.name.toString.startsWith("set") =>
          (s.name.toString.drop(3), s.tree.asInstanceOf[DefDef])
      }.toMap

    if(getters.isEmpty) {
      val mess =
        s"""Class ${sym.name} has not getter"""
      report.throwError(mess)
    }

    getters.foreach { case (name, info) =>
      val setter: DefDef =
        setters // Map[String, DefDef]
          .get(name)
          .getOrElse {
            val mess =
              s"""JavaBean contained a getter for field $name""" +
                """ but did not contain a matching setter."""
            report.throwError(mess)
          }

      val resType: TypeRepr = info.returnTpt.tpe
      setter.paramss.head match {
        case TypeParamClause(params: List[TypeDef]) => report.throwError(s"JavaBean setter for field $name has type parameters")
        case TermParamClause(head :: _) => 
          val tpe = head.tpt.tpe
          if (resType != tpe) {
            val mess =
              s"""JavaBean contained setter for field $name that had a mismatching type.
                    |  found:    $tpe
                    |  expected: $resType""".stripMargin
            report.throwError(mess)
          }
      }
    }
  }

  private def isJavaBeanImpl[T](using Quotes, Type[T]): Expr[IsJavaBean[T]] = {
    import quotes.reflect._
    val sym = TypeTree.of[T].symbol
    // TODO: scala3 - check if symbol is a Java class ?
    checkGetterAndSetters(sym)
    '{new IsJavaBean[T]{}}
  }

  inline given isJavaBean[T]: IsJavaBean[T] = {
    ${ isJavaBeanImpl[T] }
  }

  def apply[T](using i: IsJavaBean[T]): IsJavaBean[T] = i
}
