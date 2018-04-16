/*
 * Copyright 2016 Spotify AB.
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

import java.io.BufferedReader

import org.apache.beam.sdk.options.PipelineOptionsFactory
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.{scalaVersion, scioVersion}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.commons.lang.StringEscapeUtils

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.{IR, JPrintWriter}

/**
 * ScioILoop - core of Scio REPL.
 * @param scioClassLoader [[ScioReplClassLoader]] used for runtime/in-memory classloading
 * @param args user arguments for Scio REPL
 */
class ScioILoop(scioClassLoader: ScioReplClassLoader,
                args: List[String],
                reader: Option[BufferedReader],
                out: JPrintWriter)
  extends ILoopCompat(reader, out) {

  private var scioIsInitialized = false

  welcome()

  def this(scioCL: ScioReplClassLoader, args: List[String]) =
    this(scioCL, args, None, new JPrintWriter(Console.out, true))

  // Fail fast for illegal arguments
  try {
    PipelineOptionsFactory.fromArgs(args: _*).create()
  } catch {
    case e: Throwable =>
      echo(e.getMessage)
      sys.exit(1)
  }

  settings = new GenericRunnerSettings(echo)

  override def printWelcome() {}

  override def prompt: String = if (scioIsInitialized) {
    Console.GREEN + "\nscio> " + Console.RESET
  } else {
    ""
  }

  // Options for creating new Scio contexts
  private var scioOpts: Array[String] = args.toArray

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
    val sc = if (name.nonEmpty) name else "sc"
    val rsc = "com.spotify.scio.repl.ReplScioContext"
    val opts = optsFromArgs(scioOpts)
    val nextReplJar = StringEscapeUtils.escapeJava(scioClassLoader.getNextReplCodeJarPath)
    intp.beQuietDuring {
      intp.interpret(s"""val $sc: ScioContext = new $rsc($opts, List("$nextReplJar"))""")
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
      val result = intp.beQuietDuring {
        intp.interpret(optsFromArgs(newOpts))
      }
      if (result == IR.Success) {
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

  // =======================================================================
  // Initialization
  // =======================================================================

  private def optsFromArgs(args: Seq[String]): String = {
    val factory = "org.apache.beam.sdk.options.PipelineOptionsFactory"
    val argsStr = args.mkString("\"", "\", \"", "\"")
    s"""$factory.fromArgs($argsStr).create()"""
  }

  private def welcome(): Unit = {
    val ascii = """Welcome to
                  |                 _____
                  |    ________________(_)_____
                  |    __  ___/  ___/_  /_  __ \
                  |    _(__  )/ /__ _  / / /_/ /
                  |    /____/ \___/ /_/  \____/""".stripMargin + "   version " + scioVersion + "\n"
    echo(ascii)

    val p = scala.util.Properties
    echo("Using Scala version %s (%s, Java %s)".format(scalaVersion, p.javaVmName, p.javaVersion))

    echo(
      """
        |Type in expressions to have them evaluated.
        |Type :help for more information.
      """.stripMargin)
  }

  private def addImports(): IR.Result =
    intp.interpret(
      """
        |import com.spotify.scio.{io => _, _}
        |import com.spotify.scio.avro._
        |import com.spotify.scio.bigquery._
        |import com.spotify.scio.repl._
        |import scala.concurrent.ExecutionContext.Implicits.global
      """.stripMargin)

  private def createBigQueryClient(): IR.Result = {
    def create(projectId: String): IR.Result = {
      val r = intp.interpret(s"""val bq = BigQueryClient("$projectId")""")
      echo(s"BigQuery client available as 'bq'")
      r
    }

    val key = BigQueryClient.PROJECT_KEY

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
        IR.Success
      }
    }
  }

  private def loadIoCommands(): IR.Result = {
    intp.interpret(
      """
        |val _ioCommands = new com.spotify.scio.repl.IoCommands(sc.options)
        |import _ioCommands._
      """.stripMargin)
  }


  override def createInterpreter(): Unit = {
    super.createInterpreter()
    intp.beQuietDuring {
      addImports()
      createBigQueryClient()
      newScioCmdImpl("sc")
      loadIoCommands()
    }
    if (in == null) {
      sys.error("Could not initialize Scio interpreter - abort." +
        "One possible reason is inconsistent Scala versions. Please use the exact same version of" +
        " Scala as scio-repl.")
    }
    scioIsInitialized = true
    out.print(prompt)
    out.flush()
  }

}
