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

  settings = new GenericRunnerSettings({ s => echo(s) })

  override def printWelcome() {
    echo("Welcome to Scio REPL!")
  }


  // =======================================================================
  // Scio REPL magic commands:
  // =======================================================================

  /**
   * Hidden magics/helpers for REPL session jars.
   */
  private val createJarCmd = LoopCommand.nullary("createJar", "creates Scio REPL runtime jar",
    () => { Result.resultFromString(scioClassLoader.createReplCodeJar) } )
  private val getNextJarCmd = LoopCommand.nullary("nextJar", "gets next path of Scio REPL jar",
    () => { Result.resultFromString(scioClassLoader.getNextReplCodeJarPath) } )

  /**
   * REPL magic to get new Scio context, creates new Scio context based on arguments given
   * to scio REPL for the user. User may specify name of the val holding context, otherwise defaults
   * to `sc`.
   */
  private def getNewScioContextCmdImpl(scioContextVal: String ) = {
    val nextReplJar = scioClassLoader.getNextReplCodeJarPath
    val scioContextName = if (scioContextVal.nonEmpty) scioContextVal else "sc"
    intp.beQuietDuring({
      intp.interpret("val __scio__opts__ = " + args.map('"' + _ + '"'))
      intp.interpret("val __scio__df__opts__ = PipelineOptionsFactory.fromArgs(__scio__opts__.toArray).as(classOf[DataflowPipelineOptions])")

      intp.interpret ("val " + scioContextName +
        " = new ReplScioContext(__scio__df__opts__, List(\"" + nextReplJar + "\"), None)")
    })
    this.echo("Scio context available as '" + scioContextName + "'")
    Result.default
  }

  private val getNewScioContextCmd = LoopCommand.cmd("newScio",
                                                     "<[scio-context-val-name] | sc>",
                                                     "gets new Scio context",
                                                     getNewScioContextCmdImpl)

  /**
   * REPL magic to get new __local__ Scio context. User may specify name of the val holding context,
   * otherwise defaults to `sc`.
   */
  private def getNewLocalScioContextCmdImpl(scioContextVal: String) = {
    val scioContextName = if (scioContextVal.nonEmpty) scioContextVal else "sc"
    intp.beQuietDuring({

      // TODO: pass BQ settings + non distributed settings
      intp.interpret("val " + scioContextName +
        " = ScioContext()")
    })
    this.echo("Local Scio context available as '" + scioContextName + "'")
    Result.default
  }

  private val getNewLocalScioContextCmd = LoopCommand.cmd("newLocalScio",
                                                          "<[scio-context-val-name] | sc>",
                                                          "gets new local Scio context",
                                                          getNewLocalScioContextCmdImpl)

  /**
   * REPL magic to run Scio context given it's value name (defaults to `sc`). It will take care of
   * dumping in-memory classes to local jar.
   */
  private def getResultCmdImpl(scioContextVal: String) = {
    scioClassLoader.createReplCodeJar
    val scioResult = scioContextVal match {
      case v if !v.isEmpty => intp.interpret(v + ".close")
      case _ => intp.interpret("sc.close")
    }
    Result.default
  }

  private val getResultCmd = LoopCommand.cmd("runScio",
                                             "<[scio-context-val-name] | sc>",
                                             "run Scio pipeline",
                                             getResultCmdImpl)

  private val scioCommands = List(getNewScioContextCmd, getNewLocalScioContextCmd)

  // TODO: find way to inject those into power commands. For now unused.
  private val scioPowerCommands = List(createJarCmd, getNextJarCmd, getResultCmd)

  /**
   * Change the shell prompt to custom Scio prompt.
   *
   * @return a prompt string to use for this REPL
   */
  override def prompt: String = Console.GREEN + "\nscio> " + Console.RESET

  private def addImports(ids: String*): IR.Result =
    if (ids.isEmpty) IR.Success else intp.interpret("import " + ids.mkString(", "))

  private def createBigQueryClient: IR.Result = {
    val key = BigQueryClient.PROJECT_KEY
    if (sys.props(key) == null) {
      echo(s"System property '$key' not set. BigQueryClient is not available.")
      echo("Set it with '-D" + key + "=my-project' command line argument.")
    } else {
      intp.interpret("val bq = BigQueryClient()")
      echo(s"BigQuery client available as 'bq'")
    }
    IR.Success
  }

  /**
   * Gets the list of commands that this REPL supports.
   *
   * @return a list of the command supported by this REPLs
   */
  override def commands: List[LoopCommand] = super.commands ++ scioCommands

  protected def imports: List[String] = List(
    "com.spotify.scio.repl.ReplScioContext",
    "com.spotify.scio._",
    "com.spotify.scio.bigquery._",
    "com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptions, PipelineOptionsFactory}",
    "com.spotify.scio.experimental._")

  override def createInterpreter() {
    super.createInterpreter()
    this.echo("Loading ... ")
    intp.beQuietDuring {
      addImports(imports: _*)
      createBigQueryClient
      getNewScioContextCmdImpl("sc")
    }

  }
}
