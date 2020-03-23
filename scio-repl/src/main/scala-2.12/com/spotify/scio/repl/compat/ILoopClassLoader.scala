package com.spotify.scio.repl.compat

import scala.tools.nsc.interpreter.ILoop

private[repl] trait ILoopClassLoader {

  protected var scioREPL: ILoop = _

  def setRepl(repl: ILoop): Unit = scioREPL = repl

}
