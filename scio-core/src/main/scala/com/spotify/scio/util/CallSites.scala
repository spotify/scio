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

package com.spotify.scio.util

import scala.collection.mutable.{Map => MMap}

private[scio] object CallSites {

  private val ns = "com.spotify.scio."

  private val methodMap = Map("$plus$plus" -> "++")

  private val nameCache = MMap.empty[String, Int]

  private def isExternalClass(c: String): Boolean =
    // Not in our code base or an interpreter
    (!c.startsWith(ns) && !c.startsWith("scala.")) ||
      c.startsWith(ns + "examples.") || // unless if it's in examples
      c.startsWith(ns + "values.AccumulatorTest") // or this test

  def getAppName: String = {
    Thread.currentThread().getStackTrace
      .drop(1)
      .find(e => isExternalClass(e.getClassName))
      .map(_.getClassName).getOrElse("unknown")
      .split("\\.").last.replaceAll("\\$$", "")
  }

  def getCurrent: String = {
    val stack = new Exception().getStackTrace.drop(1)
    val p = stack.indexWhere(e => isExternalClass(e.getClassName))

    val k = stack(p - 1).getMethodName
    val method = methodMap.getOrElse(k, k)
    val file = stack(p).getFileName
    val line = stack(p).getLineNumber
    val name = s"$method@{$file:$line}"

    if (!nameCache.contains(name)) {
      nameCache(name) = 1
      name
    } else {
      nameCache(name) += 1
      name + nameCache(name)
    }
  }

}
