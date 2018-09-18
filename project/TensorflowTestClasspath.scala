/*
 * Copyright 2017 Spotify AB.
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

import sbt._, Keys._

object TensorflowTestClasspath {

  /**
   * Workaround a JNI issue by forcing tensorflow-java to be loaded with the
   * system classloader. This garantuee that the java library and the native lib
   * share the same lifecycle.
   * @see: https://github.com/spotify/scio/issues/1137
   * @see: https://github.com/tensorflow/tensorflow/issues/19298
   */
  def create(parent: ClassLoader, dpc: Classpath): ClassLoader = {
    import java.net.URLClassLoader
    val tf =
      dpc.collect {
        case d if d.data.toString.contains("libtensorflow") =>
          d.data.toURI.toURL
      }

    val sysloader =
      ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val sysclass = classOf[URLClassLoader]
    val systemAddJar = sysclass.getDeclaredMethod("addURL", classOf[URL])
    systemAddJar.setAccessible(true)
    tf.foreach { f =>
      systemAddJar.invoke(sysloader, f.toURI().toURL())
    }
    parent
  }
}
