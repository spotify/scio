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

import java.io.{OutputStream, PrintStream}

import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.spotify.scio.{ScioContext, ScioResult}

class ReplScioContext(options: PipelineOptions,
                      artifacts: List[String])
  extends ScioContext(options, artifacts) {

  private lazy val nullout = new PrintStream(new OutputStream() {
    override def write(b: Int) = {}
    override def write(b: Array[Byte]) = {}
    override def write(b: Array[Byte], off: Int, len: Int) = {}
  })

  /** Enhanced version that dumps REPL session jar. */
  override def close(): ScioResult = {
    createJar()
    super.close()
  }

  /** Ensure an operation is called before the pipeline is closed. */
  override private[scio] def pipelineOp[T](body: => T): T = {
    require(!this.isClosed,
      "ScioContext already closed, use :newScio <[context-name] | sc> to create new context")
    super.pipelineOp(body)
  }

  private def createJar(): Unit = {
    // scalastyle:off structural.type
    import scala.language.reflectiveCalls
    this.getClass.getClassLoader.asInstanceOf[{def createReplCodeJar: String}].createReplCodeJar
    // scalastyle:on structural.type
  }

}
