package com.spotify.scio.repl.compat

import scala.tools.nsc.interpreter.ILoop
import com.spotify.scio.repl.ScioILoop

private[repl] trait ILoopClassLoader {

  protected var scioREPL: ScioILoop = _

  def setRepl(repl: ScioILoop): Unit = scioREPL = repl

}
