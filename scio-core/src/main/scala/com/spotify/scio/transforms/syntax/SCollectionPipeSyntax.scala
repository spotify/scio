/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.transforms.syntax

import java.io.File

import com.spotify.scio.values.SCollection
import com.spotify.scio.transforms.PipeDoFn
import org.apache.beam.sdk.transforms.ParDo

import scala.jdk.CollectionConverters._

trait SCollectionPipeSyntax {

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with pipe methods. */
  implicit class PipeSCollection(private val self: SCollection[String]) {

    /**
     * Pipe elements through an external command via StdIn & StdOut.
     * @param command
     *   the command to call
     * @param environment
     *   environment variables
     * @param dir
     *   the working directory of the subprocess
     * @param setupCmds
     *   setup commands to be run before processing
     * @param teardownCmds
     *   tear down commands to be run after processing
     */
    def pipe(
      command: String,
      environment: Map[String, String] = null,
      dir: File = null,
      setupCmds: Seq[String] = null,
      teardownCmds: Seq[String] = null
    ): SCollection[String] = {
      val env = if (environment == null) null else environment.asJava
      val sCmds = if (setupCmds == null) null else setupCmds.asJava
      val tCmds = if (teardownCmds == null) null else teardownCmds.asJava
      self.applyTransform(ParDo.of(new PipeDoFn(command, env, dir, sCmds, tCmds)))
    }

    /**
     * Pipe elements through an external command via StdIn & StdOut.
     * @param cmdArray
     *   array containing the command to call and its arguments
     * @param environment
     *   environment variables
     * @param dir
     *   the working directory of the subprocess
     * @param setupCmds
     *   setup commands to be run before processing
     * @param teardownCmds
     *   tear down commands to be run after processing
     */
    def pipe(
      cmdArray: Array[String],
      environment: Map[String, String],
      dir: File,
      setupCmds: Seq[Array[String]],
      teardownCmds: Seq[Array[String]]
    ): SCollection[String] = {
      val env = if (environment == null) null else environment.asJava
      val sCmds = if (setupCmds == null) null else setupCmds.asJava
      val tCmds = if (teardownCmds == null) null else teardownCmds.asJava
      self.applyTransform(ParDo.of(new PipeDoFn(cmdArray, env, dir, sCmds, tCmds)))
    }
  }
}
