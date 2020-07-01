/*
 * Copyright 2020 Spotify AB.
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

import scala.reflect.io.File
import scala.tools.nsc.util.ClassPath
import scala.tools.nsc.{GenericRunnerCommand, MainGenericRunner}

trait ScioGenericRunner extends MainGenericRunner {

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
    val command = new GenericRunnerCommand(args.toList, _ => ())

    val fromSbt = Thread.currentThread.getStackTrace.exists { elem =>
      elem.getClassName.startsWith("sbt.Run")
    }
    command.settings.usejavacp.value = !fromSbt

    ScioReplClassLoader
      .classLoaderURLs(Thread.currentThread.getContextClassLoader)
      .map(_.getPath)
      .foreach(command.settings.classpath.append)

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

    val scioClassLoader = ScioReplClassLoader(command.settings.classpathURLs.toArray)
    val repl = new ScioILoop(command, args.toList)

    command.settings.Yreploutdir.value = ""
    // Workaround for https://github.com/spotify/scio/issues/867
    command.settings.Yreplclassbased.value = true
    // Force the repl to be synchronous, so all cmds are executed in the same thread
    command.settings.Yreplsync.value = true
    // Set classloader chain - expose top level abstract class loader down
    // the chain to allow for readObject and latestUserDefinedLoader
    // See https://gist.github.com/harrah/404272
    command.settings.embeddedDefaults(scioClassLoader)

    repl.run(command.settings)
  }
}
