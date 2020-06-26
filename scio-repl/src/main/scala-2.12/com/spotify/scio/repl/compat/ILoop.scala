package com.spotify.scio.repl.compat

import java.io.{PrintWriter => JPrintWriter}

import scala.tools.nsc.interpreter
import scala.tools.nsc.CompilerCommand
import scala.tools.nsc.{CompilerCommand, Settings}

abstract class ILoop(
  command: CompilerCommand
) extends interpreter.ILoop(None, new JPrintWriter(Console.out, true)) {

  settings = command.settings

  def welcome: String

  override def printWelcome(): Unit = echo(welcome)

  def run(settings: Settings): Boolean = process(settings)

  def initCommand(): Unit

  override def createInterpreter(): Unit = {
    super.createInterpreter()
    initCommand()
    out.print(prompt)
    out.flush()
  }
}
