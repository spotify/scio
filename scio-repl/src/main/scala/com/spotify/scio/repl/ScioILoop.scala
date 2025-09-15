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

import com.spotify.scio.BuildInfo
import com.spotify.scio.bigquery.BigQuerySysProps
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory

import scala.tools.nsc.CompilerCommand
import scala.tools.nsc.interpreter.Results
import scala.collection.compat.immutable.ArraySeq

/**
 * ScioILoop - core of Scio REPL.
 * @param scioClassLoader
 *   [[ScioReplClassLoader]] used for runtime/in-memory classloading
 * @param args
 *   user arguments for Scio REPL
 */
class ScioILoop(command: CompilerCommand, args: List[String]) extends compat.ILoop(command) {

  // Fail fast for illegal arguments
  try {
    PipelineOptionsFactory.fromArgs(args: _*).create()
  } catch {
    case e: Throwable =>
      echo(e.getMessage)
      sys.exit(1)
  }

  override lazy val prompt: String =
    Console.GREEN + "\nscio> " + Console.RESET

  // Options for creating new Scio contexts
  private var scioOpts: Array[String] = args.toArray

  // =======================================================================
  // Scio REPL magic commands:
  // =======================================================================

  /**
   * REPL magic to get a new Scio context using arguments from the command line or :scioOpts. User
   * may specify a name for the context val, default is `sc`.
   */
  private def newScioCmdImpl(name: String) = {
    val sc = if (name.nonEmpty) name else "sc"
    val rsc = "com.spotify.scio.repl.ReplScioContext"
    val opts = optsFromArgs(ArraySeq.unsafeWrapArray(scioOpts))
    intp.beQuietDuring {
      intp.interpret(
        s"""val $sc: ScioContext = $rsc($opts, \"\"\"${outputDir.path}\"\"\")"""
      )
      ()
    }
    this.echo(s"Scio context available as '$sc'")
    Result.default
  }

  private val newScioCmd =
    LoopCommand.cmd("newScio", "<[context-name] | sc>", "get a new Scio context", newScioCmdImpl)

  /**
   * REPL magic to get a new __local__ Scio context. User may specify a name for the context val,
   * default is `sc`.
   */
  private def newLocalScioCmdImpl(name: String) = {
    val sc = if (name.nonEmpty) name else "sc"
    intp.beQuietDuring {
      // TODO: pass BQ settings + non distributed settings
      intp.interpret(s"val $sc = ScioContext()")
      ()
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
        intp.interpret(optsFromArgs(ArraySeq.unsafeWrapArray(newOpts)))
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

  private val scioCommands = List(newScioCmd, newLocalScioCmd, scioOptsCmd)

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

  override def welcome: String = {
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

  override def initCommand(): Unit = {
    intp.beQuietDuring {
      addImports()
      createBigQueryClient()
      newScioCmdImpl("sc")
      loadIoCommands()
      ()
    }
  }

  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    initCommand()
  }

}
