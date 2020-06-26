package com.spotify.scio.repl.compat

import scala.tools.nsc.interpreter.shell
import scala.tools.nsc.CompilerCommand
import scala.tools.nsc.Settings

abstract class ILoop(command: CompilerCommand)
    extends shell.ILoop(shell.ShellConfig(command.settings)) {

  def initCommand(): Unit

  override def createInterpreter(interpreterSettings: Settings): Unit = {
    super.createInterpreter(interpreterSettings)
    initCommand()
    out.print(prompt)
    out.flush()
  }
}
