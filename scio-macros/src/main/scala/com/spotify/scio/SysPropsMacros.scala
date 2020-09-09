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

import scala.annotation.{compileTimeOnly, StaticAnnotation}
import scala.reflect.macros.blackbox

@compileTimeOnly(
  "enable macro paradise (2.12) or -Ymacro-annotations (2.13) to expand macro annotations"
)
final class registerSysProps extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro registerSysPropsMacro.impl
}

private object registerSysPropsMacro {
  def impl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val traitT = tq"_root_.com.spotify.scio.SysProps"

    annottees.map(_.tree) match {
      case List(q"$mod object $name extends ..$parents { ..$body }") =>
        val vars = body.collect { case ValDef(_, _, _, rhs) =>
          c.Expr(rhs)
        }

        val propertiesMethod =
          q"""override def properties: List[SysProp] = List(..$vars)"""

        c.Expr[Any](q"""
            $mod object $name extends ..$parents with $traitT {
              $propertiesMethod
              ..$body
            }
            """)
      case t => c.abort(c.enclosingPosition, s"Invalid annotation $t")
    }
  }
}
