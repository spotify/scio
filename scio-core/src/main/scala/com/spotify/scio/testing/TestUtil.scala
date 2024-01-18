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

package com.spotify.scio.testing

import java.util.UUID

private[scio] object TestUtil {

  def newTestId(): String = newTestId(None)
  def newTestId(clazz: Class[_]): String = newTestId(clazz.getSimpleName)
  def newTestId(name: String): String = {
    require(name.nonEmpty && !name.contains('-'), s"Invalid test name $name")
    newTestId(Some(name))
  }

  private def newTestId(suffix: Option[String]): String = {
    val id = UUID.randomUUID().toString.replaceAll("-", "")
    val parts = Seq("JobTest") ++ suffix ++ Seq(id)
    parts.mkString("-")
  }

  def isTestId(appName: String): Boolean =
    "JobTest(-[^-]+)?-[a-z0-9]+".r.pattern.matcher(appName).matches()
}
