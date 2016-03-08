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

import java.io.BufferedReader

import com.spotify.scio.bigquery.BigQueryClient

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.{IR, JPrintWriter}

/**
 * ScioILoop - core of Scio REPL.
 * @param scioClassLoader [[ScioReplClassLoader]] used for runtime/in-memory classloading
 * @param args user arguments for Scio REPL
 */
class ScioILoop(scioClassLoader: ScioReplClassLoader,
                args: List[String],
                in: Option[BufferedReader],
                out: JPrintWriter)
  extends ILoopCompat(in, out) {

  def this(scioCL: ScioReplClassLoader, args: List[String]) = this(scioCL, args, None, new JPrintWriter(Console.out, true))

  settings = new GenericRunnerSettings(echo)

  override def printWelcome() {
    echo("Welcome to Scio REPL!")
  }

  override def prompt: String = Console.GREEN + "\nscio> " + Console.RESET

  var scioOpts = args.toArray

  // =======================================================================
  // Scio REPL magic commands:
  // =======================================================================

  // Hidden magics/helpers for REPL session jars.

  private val createJarCmd = LoopCommand.nullary(
    "createJar", "create a Scio REPL runtime jar",
    () => { Result.resultFromString(scioClassLoader.createReplCodeJar) } )

  private val getNextJarCmd = LoopCommand.nullary(
    "nextJar", "get the path of the next Scio REPL runtime jar",
    () => { Result.resultFromString(scioClassLoader.getNextReplCodeJarPath) } )

  /**
   * REPL magic to get a new Scio context using arguments from the command line or :scioOpts.
   * User may specify a name for the context val, default is `sc`.
   */
  private def newScioCmdImpl(name: String) = {
    updateOpts(scioOpts)

    val nextReplJar = scioClassLoader.getNextReplCodeJarPath
    val sc = if (name.nonEmpty) name else "sc"
    intp.beQuietDuring {
      intp.interpret ("val " + sc + " = new ReplScioContext(__scio__df__opts__, List(\"" + nextReplJar + "\"), None)")
      intp.interpret(sc + ".setName(\"sciorepl\")")
    }
    this.echo("Scio context available as '" + sc + "'")
    Result.default
  }

  private val newScioCmd = LoopCommand.cmd(
    "newScio", "<[context-name] | sc>", "get a new Scio context", newScioCmdImpl)

  /**
   * REPL magic to get a new __local__ Scio context.
   * User may specify a name for the context val, default is `sc`.
   */
  private def newLocalScioCmdImpl(name: String) = {
    val sc = if (name.nonEmpty) name else "sc"
    intp.beQuietDuring {
      // TODO: pass BQ settings + non distributed settings
      intp.interpret(s"val $sc = ScioContext()")
    }
    this.echo(s"Local Scio context available as '$sc'")
    Result.default
  }

  private val newLocalScioCmd = LoopCommand.cmd(
    "newLocalScio", "<[context-name] | sc>", "get a new local Scio context", newLocalScioCmdImpl)

  /** REPL magic to show or update Scio options. */
  private def scioOptsCmdImpl(args: String) = {
    if (args.trim.nonEmpty) {
      // update options
      val newOpts = args.split("\\s+")
      if (updateOpts(newOpts) == IR.Success) {
        scioOpts = newOpts
        echo("Scio options updated. Use :newScio to get a new Scio context.")
      }
    } else {
      if (scioOpts.isEmpty) {
        echo("Scio options is empty")
      } else {
        echo("Scio options: " + scioOpts.mkString(" "))
      }
    }
    Result.default
  }

  private val scioOptsCmd = LoopCommand.cmd(
    "scioOpts", "<[opts]>", "show or update Scio options", scioOptsCmdImpl)

  /**
   * REPL magic to run a Scio context.
   * User may specify a name for the context val, default is `sc`.
   * It will take care of dumping in-memory classes to local jar.
   */
  private def runScioCmdImpl(name: String) = {
    scioClassLoader.createReplCodeJar
    val sc = if (name.nonEmpty) name else "sc"
    val scioResult = intp.interpret(s"$sc.close()")
    Result.default
  }

  private val runScioCmd = LoopCommand.cmd(
    "runScio", "<[context-name] | sc>", "run Scio pipeline", runScioCmdImpl)

  private val scioCommands = List(newScioCmd, newLocalScioCmd, scioOptsCmd)

  // TODO: find way to inject those into power commands. For now unused.
  private val scioPowerCommands = List(createJarCmd, getNextJarCmd, runScioCmd)

  override def commands: List[LoopCommand] = super.commands ++ scioCommands

  private def updateOpts(args: Array[String]): IR.Result = {
    intp.beQuietDuring {
      intp.interpret("val __scio__opts__ = " + args.mkString("Array(\"", "\", \"", "\")"))
      intp.interpret("val __scio__df__opts__ = " +
        "PipelineOptionsFactory.fromArgs(__scio__opts__).as(classOf[DataflowPipelineOptions])")
    }
  }

  private def addImports(): IR.Result = {
    val imports = List(
      "com.spotify.scio.repl.ReplScioContext",
      "com.spotify.scio._",
      "com.spotify.scio.bigquery._",
      "com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptions, PipelineOptionsFactory}",
      "com.spotify.scio.experimental._")
    imports.foreach(p => intp.interpret(s"import $p"))
    IR.Success
  }

  private def createBigQueryClient(): IR.Result = {
    val key = BigQueryClient.PROJECT_KEY
    if (sys.props(key) == null) {
      echo(s"System property '$key' not set. BigQueryClient is not available.")
      echo("Set it with '-D" + key + "=<PROJECT-NAME>' command line argument.")
    } else {
      intp.interpret("val bq = BigQueryClient()")
      echo(s"BigQuery client available as 'bq'")
    }
    IR.Success
  }

  override def createInterpreter() {
    super.createInterpreter()
    this.echo("Loading ... ")
    intp.beQuietDuring {
      addImports()
      createBigQueryClient()
      newScioCmdImpl("sc")
    }
  }

}
