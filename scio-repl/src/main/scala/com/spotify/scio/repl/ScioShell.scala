/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.repl

import scala.tools.nsc.{GenericRunnerCommand, MainGenericRunner}

/**
 * A entry-point/runner for a Scala REPL providing functionality extensions specific to working with
 * Scio.
 */
trait BaseScioShell extends MainGenericRunner {

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

    // if running from the assembly, need to explicitly tell it to use java classpath
    command.settings.usejavacp.value = true
    command.settings.classpath.append(System.getProperty("java.class.path"))

    // Repl assembly includes paradise's scalac-plugin.xml - required for BigQuery macro
    val thisJar = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    command.settings.plugin.appendToValue(thisJar)

    // Useful settings for for debugging, dumping class files etc:
    /* command.settings.debug.value = true
    command.settings.Yreploutdir.tryToSet(List(""))
    command.settings.Ydumpclasses.tryToSet(List("")) */

    // Force the repl to be synchronous, so all cmds are executed in the same thread
    command.settings.Yreplsync.value = true

    val scioClassLoader = new ScioReplClassLoader(command.settings.classpathURLs.toArray,
      null,
      Thread.currentThread.getContextClassLoader)

    val repl = new ScioILoop(scioClassLoader, args.toList)
    scioClassLoader.setRepl(repl)

    // Set classloader chain - expose top level abstract class loader down
    // the chain to allow for readObject and latestUserDefinedLoader
    command.settings.embeddedDefaults(scioClassLoader)

    repl.process(command.settings)
  }

  /** Runs an instance of the shell. */
  def main(args: Array[String]) {
    val retVal = process(args)
    if (!retVal) {
      sys.exit(1)
    }
  }
}

object ScioShell extends BaseScioShell
