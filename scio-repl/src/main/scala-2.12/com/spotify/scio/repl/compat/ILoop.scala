/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.repl.compat

import java.io.{PrintWriter => JPrintWriter}

import scala.tools.nsc.interpreter
import scala.tools.nsc.CompilerCommand
import scala.tools.nsc.{CompilerCommand, Settings}
import scala.reflect.io.AbstractFile

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

  def outputDir: AbstractFile = intp.replOutput.dir

}
