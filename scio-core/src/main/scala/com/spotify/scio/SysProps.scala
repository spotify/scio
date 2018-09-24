package com.spotify.scio

import com.google.common.reflect.ClassPath

final case class SysProp(flag: String, description: String) {
  def value[T >: String](default: => T): T = sys.props.getOrElse(flag, default)

  def value: String = sys.props(flag)

  def valueOption[T >: String]: Option[T] = sys.props.get(flag)

  // scalastyle:off method.name
  def value_=[T >: String](t: T): Unit =
    sys.props(flag) = t.asInstanceOf[String]
  // scalastyle:on method.name
}

trait SysProps {
  def properties: List[SysProp]
}

object SysProps {
  import scala.collection.JavaConverters._
  import scala.reflect.runtime.universe

  def properties: Iterable[SysProps] = {
    val classLoader = Thread.currentThread().getContextClassLoader
    val runtimeMirror = universe.runtimeMirror(classLoader)
    ClassPath
      .from(classLoader)
      .getAllClasses
      .asScala
      .filter(_.getName.endsWith("SysProps"))
      .flatMap { clsInfo =>
        try {
          val cls = clsInfo.load()
          cls.getMethod("properties")
          val module = runtimeMirror.staticModule(cls.getName)
          val obj = runtimeMirror.reflectModule(module)
          Some(obj.instance.asInstanceOf[SysProps])
        } catch {
          case _: Throwable => None
        }
      }
  }
}

object CoreSysProps extends SysProps {

  val Project = SysProp("project", "")
  val Home = SysProp("java.home", "java home directory")
  val TmpDir = SysProp("java.io.tmpdir", "java temporary directory")
  val User = SysProp("user.name", "system username")
  val UserDir = SysProp("user.dir", "user dir")

  override def properties: List[SysProp] = List(TmpDir)
}
