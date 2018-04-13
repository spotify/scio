/*
 * Copyright 2018 Spotify AB.
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

import java.util.function.BiFunction

import com.google.common.collect.Maps

private[scio] object CallSites {

  private val scioNs = "com.spotify.scio."
  private val beamNs = "org.apache.beam."

  private val methodMap = Map("$plus$plus" -> "++")
  private val nameCache = Maps.newConcurrentMap[String, Int]()

  private def isExternalClass(c: String): Boolean =
    // Not in our code base or an interpreter
    (!c.startsWith(scioNs) && !c.startsWith("scala.") && !c.startsWith(beamNs)) ||
      c.startsWith(scioNs + "examples.") || // unless if it's in examples
      c.startsWith(scioNs + "values.ClosureTest") // or this test

  private def isTransform(e: StackTraceElement): Boolean =
    e.getClassName == scioNs + "values.SCollectionImpl" && e.getMethodName == "transform"

  def getAppName: String = {
    Thread.currentThread().getStackTrace
      .drop(1)
      .find(e => isExternalClass(e.getClassName))
      .map(_.getClassName).getOrElse("unknown")
      .split("\\.").last.replaceAll("\\$$", "")
  }

  /** Get a unique identifier for the current call site. */
  def getCurrent: String = {
    val name = getCurrentName

    val n = nameCache.merge(name, 1, new BiFunction[Int, Int, Int] {
      override def apply(t: Int, u: Int): Int = t + u
    })
    if (n == 1) name else name + 1
  }

  /** Get current call site name in the form of "method@{file:line}". */
  def getCurrentName: String = {
    val stack = new Exception().getStackTrace.drop(1)

    // find first stack outside of Scio or SDK
    var pExt = stack.indexWhere(e => isExternalClass(e.getClassName))

    val pTransform = stack.indexWhere(isTransform)
    if (pTransform < pExt && pTransform > 0) {
      val m = stack(pExt - 1).getMethodName  // method implemented with transform
      val _p = stack.take(pTransform).indexWhere(e => e.getClassName.contains(m))
      if (_p > 0) {
        pExt = _p
      }
    }

    val k = stack(pExt - 1).getMethodName
    val method = methodMap.getOrElse(k, k)
    val file = stack(pExt).getFileName
    val line = stack(pExt).getLineNumber
    s"$method@{$file:$line}"
  }

}
