package com.spotify.scio.repl.compat

import scala.tools.nsc.interpreter.shell

private[repl] trait ILoopClassLoader {

  protected var scioREPL: shell.ILoop = _

  def setRepl(repl: shell.ILoop): Unit = scioREPL = repl

}
