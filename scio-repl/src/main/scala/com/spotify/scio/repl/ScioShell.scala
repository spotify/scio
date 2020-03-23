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

package com.spotify.scio.repl

import java.lang.invoke.{MethodHandles, MethodType}

import com.spotify.scio.bigquery.BigQuerySysProps

import scala.reflect.io.File
import scala.tools.nsc.util.ClassPath
import scala.tools.nsc.{GenericRunnerCommand, MainGenericRunner}

/**
 * A entry-point/runner for a Scala REPL providing functionality extensions specific to working with
 * Scio.
 */
object BaseScioShell {
  val JDK9OrHigher: Boolean = util.Properties.isJavaAtLeast("9")

  val BootClassLoader: ClassLoader = {
    if (!JDK9OrHigher) null
    else {
      try {
        MethodHandles
          .lookup()
          .findStatic(
            classOf[ClassLoader],
            "getPlatformClassLoader",
            MethodType.methodType(classOf[ClassLoader])
          )
          .invoke()
      } catch {
        case _: Throwable =>
          null
      }
    }
  }

  def classLoaderURLs(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null                       => Array.empty
    case u: java.net.URLClassLoader => u.getURLs ++ classLoaderURLs(cl.getParent)
    case _                          => classLoaderURLs(cl.getParent)
  }
}

trait BaseScioShell extends MainGenericRunner {
  import BaseScioShell._

  /**
   * The main entry point for executing the REPL.
   *
   * This method is lifted from [[scala.tools.nsc.MainGenericRunner]] and modified to allow
   * for custom functionality, including determining at runtime if the REPL is running,
   * and making custom REPL colon-commands available to the user.
   *
   * @param args passed from the command line
   * @return `true` if execution was successful, `false` otherwise
   */
  override def process(args: Array[String]): Boolean = {
    // Process command line arguments into a settings object, and use that to start the REPL.
    // We ignore params we don't care about - hence error function is empty
    val command = new GenericRunnerCommand(args.toList, _ => ())

    val fromSbt = Thread.currentThread.getStackTrace.exists { elem =>
      elem.getClassName.startsWith("sbt.Run")
    }
    command.settings.usejavacp.value = !fromSbt

    classLoaderURLs(Thread.currentThread.getContextClassLoader)
      .map(_.getPath)
      .foreach { path =>
        command.settings.classpath.append(path)
        command.settings.bootclasspath.append(path)
      }

    // We have to make sure that scala macros are expandable. paradise plugin has to be added to
    // -Xplugin paths. In case of assembly - paradise is included in assembly jar - thus we add
    // itself to -Xplugin. If shell is started from sbt or classpath, paradise jar has to be in
    // classpath, we find it and add it to -Xplugin.

    // Repl assembly includes paradise's scalac-plugin.xml - required for BigQuery macro
    // There should be no harm if we keep this for sbt launch.
    val thisJar =
      this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    command.settings.plugin.tryToSet(List(thisJar))

    ClassPath
      .split(command.settings.classpath.value)
      .find(File(_).name.startsWith("paradise_"))
      .foreach(s => command.settings.plugin.tryToSet(List(s)))

    // Force the repl to be synchronous, so all cmds are executed in the same thread
    command.settings.Yreplsync.value = true

    // Workaround for https://github.com/spotify/scio/issues/867
    command.settings.Yreplclassbased.value = true

    val scioClassLoader = new ScioReplClassLoader(
      command.settings.classpathURLs.toArray,
      BootClassLoader
    )

    val repl = new ScioILoop(command, scioClassLoader, args.toList)
    scioClassLoader.setRepl(repl)

    // Set classloader chain - expose top level abstract class loader down
    // the chain to allow for readObject and latestUserDefinedLoader
    // See https://gist.github.com/harrah/404272
    command.settings.embeddedDefaults(scioClassLoader)

    repl.run(command.settings)
  }

  /** Runs an instance of the shell. */
  def main(args: Array[String]): Unit = {
    sys.props(BigQuerySysProps.DisableDump.flag) = "true"
    sys.props(ScioReplSysProps.MaxPrintString.flag) = "1500"

    val retVal = process(args)
    if (!retVal) {
      sys.exit(1)
    }
  }
}

object ScioShell extends BaseScioShell
