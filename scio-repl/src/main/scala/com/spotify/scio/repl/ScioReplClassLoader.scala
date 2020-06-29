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

import java.net.{URL, URLClassLoader}
import java.lang.invoke.{MethodHandles, MethodType}

object ScioReplClassLoader {
  private[this] val JDK9OrHigher: Boolean = util.Properties.isJavaAtLeast("9")

  private[this] val BootClassLoader: ClassLoader = {
    if (!JDK9OrHigher) null
    else {
      try {
        MethodHandles
          .lookup()
          .findStatic(
            classOf[ClassLoader],
            "getPlatformClassLoader",
            MethodType.methodType(classOf[ClassLoader])
          )
          .invoke()
      } catch { case _: Throwable => null }
    }
  }

  def classLoaderURLs(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null                       => Array.empty
    case u: java.net.URLClassLoader => u.getURLs ++ classLoaderURLs(cl.getParent)
    case _                          => classLoaderURLs(cl.getParent)
  }

  @inline final def apply(urls: Array[URL]) =
    new ScioReplClassLoader(urls, BootClassLoader)
}

/**
 * Class loader with option to lookup classes in REPL classloader.
 * Some help/code from Twitter Scalding.
 * @param urls classpath urls for URLClassLoader
 * @param parent parent for Scio CL - may be null to close the chain
 */
class ScioReplClassLoader(urls: Array[URL], parent: ClassLoader)
    extends URLClassLoader(urls, parent)
