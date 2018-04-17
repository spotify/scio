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

import java.io.{File, FileOutputStream}
import java.net.{URL, URLClassLoader}
import java.nio.file.Files
import java.util.jar.{JarEntry, JarOutputStream}

import org.slf4j.LoggerFactory

import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.io._

/**
 * Class loader with option to lookup classes in REPL classloader.
 * Some help/code from Twitter Scalding.
 * @param urls classpath urls for URLClassLoader
 * @param parent parent for Scio CL - may be null to close the chain
 */
class ScioReplClassLoader(urls: Array[URL], parent: ClassLoader, detachedParent: ClassLoader)
  extends URLClassLoader(urls, parent) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val replJarName = "scio-repl-session.jar"
  private var nextReplJarDir: File = genNextReplCodeJarDir

  private var scioREPL: ILoop = _

  def setRepl(repl: ILoop): Unit = scioREPL = repl

  override def loadClass(name: String): Class[_] = {
    // If contains $line - means that repl was loaded, so we can lookup
    // runtime classes
    if (name.contains("$line")) {
      logger.debug(s"Trying to load $name")
      // Don't want to use Try{} cause nonFatal handling
      val clazz: Class[_] = try {
        scioREPL.classLoader.loadClass(name)
      } catch {
        case e: Exception => {
          logger.error(s"Could not find $name in REPL classloader", e)
          null
        }
      }
      if (clazz != null) {
        logger.debug(s"Found $name in REPL classloader ${scioREPL.classLoader}")
        clazz
      } else {
        super.loadClass(name)
      }
    } else {
      super.loadClass(name)
    }
  }

  def genNextReplCodeJarDir: File = Files.createTempDirectory("scio-repl-").toFile
  def getNextReplCodeJarPath: String = new File(nextReplJarDir, replJarName).getAbsolutePath

  /**
   * Creates a jar file in a temporary directory containing the code thus far compiled by the REPL.
   *
   * @return some file for the jar created, or `None` if the REPL is not running
   */
  private[scio] def createReplCodeJar: String = {
    require(scioREPL != null, "scioREPL can't be null - set it first!")

    val virtualDirectory = scioREPL.replOutput.dir

    val tempJar = new File(nextReplJarDir, replJarName)

    // Generate next repl jar dir
    nextReplJarDir = genNextReplCodeJarDir

    val jarFile = createJar(virtualDirectory.asInstanceOf[VirtualDirectory], tempJar)
    jarFile.getPath
  }

  /**
   * Creates a jar file from the classes contained in a virtual directory.
   *
   * @param virtualDirectory containing classes that should be added to the jar
   */
  private def createJar(virtualDirectory: VirtualDirectory, jarFile: File): File = {
    val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
    try {
      addVirtualDirectoryToJar(virtualDirectory, "", jarStream)
    } finally {
      jarStream.close()
    }

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
  private def addVirtualDirectoryToJar( dir: VirtualDirectory,
                                        entryPath: String,
                                        jarStream: JarOutputStream) {
    dir.foreach { file =>
      if (file.isDirectory) {
        // Recursively descend into subdirectories, adjusting the package name as we do.
        val dirPath = entryPath + file.name + "/"
        val entry: JarEntry = new JarEntry(dirPath)
        jarStream.putNextEntry(entry)
        jarStream.closeEntry()
        addVirtualDirectoryToJar(file.asInstanceOf[VirtualDirectory], dirPath, jarStream)
      } else if (file.hasExtension("class")) {
        // Add class files as an entry in the jar file and write the class to the jar.
        val entry: JarEntry = new JarEntry(entryPath + file.name)
        jarStream.putNextEntry(entry)
        jarStream.write(file.toByteArray)
        jarStream.closeEntry()
      }
    }
  }
}
