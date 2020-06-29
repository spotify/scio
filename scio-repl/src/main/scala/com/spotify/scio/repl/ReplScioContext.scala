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

import java.io.File
import java.nio.file.Files
import java.util.jar.JarOutputStream
import java.io.FileOutputStream
import java.util.jar.JarEntry

import org.apache.beam.sdk.options.PipelineOptions
import com.spotify.scio.{ScioContext, ScioExecutionContext}

import scala.jdk.CollectionConverters._
import scala.reflect.io.AbstractFile
import scala.reflect.io.Path

object ReplScioContext {
  private[this] val ReplJarName = "scio-repl-session.jar"

  final def apply(options: PipelineOptions, outputDir: String): ReplScioContext = {
    val tempJar = new File(Files.createTempDirectory("scio-repl-").toFile, ReplJarName)
    new ReplScioContext(options, outputDir, tempJar)
  }
}

class ReplScioContext private (options: PipelineOptions, replOutputDir: String, tempJar: File)
    extends ScioContext(options, List(tempJar.toString())) {

  /** Enhanced version that dumps REPL session jar. */
  override def run(): ScioExecutionContext = {
    createJar()
    super.run()
  }

  /** Ensure an operation is called before the pipeline is closed. */
  override private[scio] def requireNotClosed[T](body: => T): T = {
    require(
      !this.isClosed,
      "ScioContext has already been executed, use :newScio <[context-name] | sc> to create new context"
    )
    super.requireNotClosed(body)
  }

  /**
   * Creates a jar file in a temporary directory containing the code thus far compiled by the REPL.
   *
   * @return some file for the jar created, or `None` if the REPL is not running
   */
  private def createJar(): String =
    createJar(AbstractFile.getDirectory(Path(replOutputDir)), tempJar).getPath()

  /**
   * Creates a jar file from the classes contained in a virtual directory.
   *
   * @param virtualDirectory containing classes that should be added to the jar
   */
  private def createJar(dir: Iterable[AbstractFile], jarFile: File): File = {
    println(dir)
    val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
    try { addVirtualDirectoryToJar(dir, "", jarStream) }
    finally { jarStream.close() }

    jarFile
  }

  /**
   * Add the contents of the specified virtual directory to a jar. This method will recursively
   * descend into subdirectories to add their contents.
   *
   * @param dir is a virtual directory whose contents should be added
   * @param entryPath for classes found in the virtual directory
   * @param jarStream for writing the jar file
   */
  private def addVirtualDirectoryToJar(
    dir: Iterable[AbstractFile],
    entryPath: String,
    jarStream: JarOutputStream
  ): Unit = dir.foreach { file =>
    if (file.isDirectory) {
      // Recursively descend into subdirectories, adjusting the package name as we do.
      val dirPath = entryPath + file.name + "/"
      jarStream.putNextEntry(new JarEntry(dirPath))
      jarStream.closeEntry()
      addVirtualDirectoryToJar(file, dirPath, jarStream)
    } else if (file.hasExtension("class")) {
      // Add class files as an entry in the jar file and write the class to the jar.
      jarStream.putNextEntry(new JarEntry(entryPath + file.name))
      jarStream.write(file.toByteArray)
      jarStream.closeEntry()
    }
  }

}
