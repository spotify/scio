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

import java.io.{PrintWriter => JPrintWriter}

import com.spotify.scio.BuildInfo
import com.spotify.scio.bigquery.BigQuerySysProps
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.text.StringEscapeUtils

import scala.tools.nsc.{CompilerCommand, Settings}
import scala.tools.nsc.interpreter.Results
import scala.tools.nsc.interpreter.ILoop

/**
 * ScioILoop - core of Scio REPL.
 * @param scioClassLoader [[ScioReplClassLoader]] used for runtime/in-memory classloading
 * @param args user arguments for Scio REPL
 */
class ScioILoop(command: CompilerCommand, scioClassLoader: ScioReplClassLoader, args: List[String])
    extends ILoop(None, new JPrintWriter(Console.out, true)) {

  // Fail fast for illegal arguments
  try {
    PipelineOptionsFactory.fromArgs(args: _*).create()
  } catch {
    case e: Throwable =>
      echo(e.getMessage)
      sys.exit(1)
  }

  settings = command.settings

  override lazy val prompt: String =
    Console.GREEN + "\nscio> " + Console.RESET

  // Options for creating new Scio contexts
  private var scioOpts: Array[String] = args.toArray

  // =======================================================================
  // Scio REPL magic commands:
  // =======================================================================

  // Hidden magics/helpers for REPL session jars.

  private val createJarCmd =
    LoopCommand.nullary(
      "createJar",
      "create a Scio REPL runtime jar",
      () => Result.resultFromString(scioClassLoader.createReplCodeJar)
    )

  private val getNextJarCmd =
    LoopCommand.nullary(
      "nextJar",
      "get the path of the next Scio REPL runtime jar",
      () => Result.resultFromString(scioClassLoader.getNextReplCodeJarPath)
    )

  /**
   * REPL magic to get a new Scio context using arguments from the command line or :scioOpts.
   * User may specify a name for the context val, default is `sc`.
   */
  private def newScioCmdImpl(name: String) = {
    val sc = if (name.nonEmpty) name else "sc"
    val rsc = "com.spotify.scio.repl.ReplScioContext"
    val opts = optsFromArgs(scioOpts)
    val nextReplJar =
      StringEscapeUtils.escapeJava(scioClassLoader.getNextReplCodeJarPath)
    intp.beQuietDuring {
      intp.interpret(s"""val $sc: ScioContext = new $rsc($opts, List("$nextReplJar"))""")
    }
    this.echo("Scio context available as '" + sc + "'")
    Result.default
  }

  private val newScioCmd =
    LoopCommand.cmd("newScio", "<[context-name] | sc>", "get a new Scio context", newScioCmdImpl)

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
    "newLocalScio",
    "<[context-name] | sc>",
    "get a new local Scio context",
    newLocalScioCmdImpl
  )

  /** REPL magic to show or update Scio options. */
  private def scioOptsCmdImpl(args: String) = {
    if (args.trim.nonEmpty) {
      // update options
      val newOpts = args.split("\\s+")
      intp.beQuietDuring {
        intp.interpret(optsFromArgs(newOpts))
        scioOpts = newOpts
        echo("Scio options updated. Use :newScio to get a new Scio context.")
      }
    } else {
      // show options
      if (scioOpts.isEmpty) {
        echo("Scio options is empty")
      } else {
        echo("Scio options: " + scioOpts.mkString(" "))
      }
    }
    Result.default
  }

  private val scioOptsCmd =
    LoopCommand.cmd("scioOpts", "<[opts]>", "show or update Scio options", scioOptsCmdImpl)

  /**
   * REPL magic to run a Scio context.
   * User may specify a name for the context val, default is `sc`.
   * It will take care of dumping in-memory classes to local jar.
   */
  private def runScioCmdImpl(name: String) = {
    scioClassLoader.createReplCodeJar
    val sc = if (name.nonEmpty) name else "sc"
    intp.interpret(s"$sc.run()")
    Result.default
  }

  private val runScioCmd =
    LoopCommand.cmd("runScio", "<[context-name] | sc>", "run Scio pipeline", runScioCmdImpl)

  private val scioCommands = List(newScioCmd, newLocalScioCmd, scioOptsCmd)

  // TODO: find way to inject those into power commands. For now unused.
  private val scioPowerCommands = List(createJarCmd, getNextJarCmd, runScioCmd)

  override def commands: List[LoopCommand] = super.commands ++ scioCommands

  // =======================================================================
  // Initialization
  // =======================================================================

  private def optsFromArgs(args: Seq[String]): String = {
    val options = args ++ args
      .find(_.startsWith("--appName"))
      .fold(Option("--appName=sciorepl"))(_ => Option.empty[String])
    val factory = "org.apache.beam.sdk.options.PipelineOptionsFactory"
    val optionsAsStr = options.mkString("\"", "\", \"", "\"")
    s"""$factory.fromArgs($optionsAsStr).create()"""
  }

  override def printWelcome(): Unit = echo(welcome())

  def welcome(): String = {
    val p = scala.util.Properties
    val ascii =
      """Welcome to
        |                 _____
        |    ________________(_)_____
        |    __  ___/  ___/_  /_  __ \
        |    _(__  )/ /__ _  / / /_/ /
        |    /____/ \___/ /_/  \____/""".stripMargin + "   version " + BuildInfo.version + "\n"

    val version = "Using Scala version %s (%s, Java %s)".format(
      BuildInfo.scalaVersion,
      p.javaVmName,
      p.javaVersion
    )

    s"""
      |$ascii
      |$version
      |
      |Type in expressions to have them evaluated.
      |Type :help for more information.
      """.stripMargin
  }

  private def addImports(): Results.Result =
    intp.interpret("""
        |import com.spotify.scio.{io => _, _}
        |import com.spotify.scio.avro._
        |import com.spotify.scio.bigquery._
        |import com.spotify.scio.bigquery.client._
        |import com.spotify.scio.repl._
        |import scala.concurrent.ExecutionContext.Implicits.global
      """.stripMargin)

  private def createBigQueryClient(): Results.Result = {
    def create(projectId: String): Results.Result = {
      val r: Results.Result = intp.interpret(s"""val bq = BigQuery("$projectId")""")
      echo(s"BigQuery client available as 'bq'")
      r
    }

    val key = BigQuerySysProps.Project.flag

    if (sys.props(key) != null) {
      create(sys.props(key))
    } else {
      val defaultProject = new DefaultProjectFactory().create(null)
      if (defaultProject != null) {
        echo(s"Using '$defaultProject' as your BigQuery project.")
        create(defaultProject)
      } else {
        echo(s"System property '$key' not set. BigQueryClient is not available.")
        echo(s"Set it with '-D$key=<PROJECT-NAME>' command line argument.")
        Results.Success
      }
    }
  }

  private def loadIoCommands(): Results.Result =
    intp.interpret("""
        |val _ioCommands = new com.spotify.scio.repl.IoCommands(sc.options)
        |import _ioCommands._
      """.stripMargin)

  override def createInterpreter(): Unit = {
    super.createInterpreter()
    intp.beQuietDuring {
      addImports()
      createBigQueryClient()
      newScioCmdImpl("sc")
      loadIoCommands()
    }
    out.print(prompt)
    out.flush()
  }

  def run(settings: Settings): Boolean = process(settings)
}
