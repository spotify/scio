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

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.ClassPath

final case class SysProp(flag: String, description: String) {
  def value(default: => String): String = sys.props.getOrElse(flag, default)

  def value: String = sys.props(flag)

  def valueOption: Option[String] = sys.props.get(flag)

  // scalastyle:off method.name
  def value_=(str: String): Unit =
    sys.props(flag) = str
  // scalastyle:on method.name

  def show: String =
    s"-D$flag=<String>\n\t$description"
}

trait SysProps {
  def properties: List[SysProp]

  def show: String = {
    val props = properties.map(p => s"  ${p.show}").mkString("\n")
    val name = this.getClass.getName.replace("$", "")
    s"$name:\n$props\n"
  }
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

@registerSysProps
object CoreSysProps {

  val Project = SysProp("project", "")
  val Home = SysProp("java.home", "java home directory")
  val TmpDir = SysProp("java.io.tmpdir", "java temporary directory")
  val User = SysProp("user.name", "system username")
  val UserDir = SysProp("user.dir", "user dir")

}
