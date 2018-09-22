package com.spotify.scio

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class registerSysProps extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro registerSysPropsMacro.impl
}

private object registerSysPropsMacro {
  def impl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val traitT = tq"_root_.com.spotify.scio.SysProps"

    annottees.map(_.tree) match {
      case List(q"$mod object $name extends ..$parents { ..$body }") =>
        val vars = body.collect {
          case ValDef(_, _, _, rhs) => c.Expr[SysProp](rhs)
        }

        val propertiesMethod =
          q"""override def properties: ${typeOf[List[SysProp]]} = List(..$vars)"""

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
