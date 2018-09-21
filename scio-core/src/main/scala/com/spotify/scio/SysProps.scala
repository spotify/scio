package com.spotify.scio

import com.google.common.reflect.ClassPath

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

final case class SysProp(flag: String, description: String) {
  def value(default: => String): String = sys.props.getOrElse(flag, default)

  def value: String = sys.props(flag)
}

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

trait SysProps {
  def envVars: List[SysProp]
}

object SysProps {
  import scala.collection.JavaConverters._
  import scala.reflect.runtime.universe

  lazy val registrars: Seq[SysProps] = {
    val classLoader = Thread.currentThread().getContextClassLoader
    ClassPath
      .from(classLoader)
      .getAllClasses
      .asScala
      .toSeq
      .filter(_.getName.endsWith("SysProps"))
      .flatMap { clsInfo =>
        try {
          val cls = clsInfo.load()
          cls.getMethod("properties")
          val runtimeMirror = universe.runtimeMirror(classLoader)
          val module = runtimeMirror.staticModule(cls.getName)
          val obj = runtimeMirror.reflectModule(module)
          Some(obj.instance.asInstanceOf[SysProps])
        } catch {
          case _: Throwable => None
        }
      }
  }
}
