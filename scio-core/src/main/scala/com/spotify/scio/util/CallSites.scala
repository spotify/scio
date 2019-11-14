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

package com.spotify.scio.util

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

private[scio] object CallSites {
  private val scioNs = "com.spotify.scio"
  private val beamNs = "org.apache.beam"

  private val methodMap = Map("$plus$plus" -> "++")
  private val nameCache = new ConcurrentHashMap[(String, String, Boolean), Int]()

  private def isExternalClass(c: String): Boolean =
    // Not in our code base or an interpreter
    (!c.startsWith(scioNs) && !c.startsWith("scala.") && !c.startsWith(beamNs)) ||
      c.startsWith(s"$scioNs.examples.") || // unless if it's in examples
      c.startsWith(s"$scioNs.values.NamedTransformTest") || // or this test
      c.startsWith(s"$scioNs.values.SimpleJob") || // or this test
      c.startsWith(s"$scioNs.values.ClosureTest") || // or this test
      // or benchmarks/ITs
      (c.startsWith(scioNs) && (c.contains("Benchmark$") || c.contains("IT")))

  private def isTransform(e: StackTraceElement): Boolean =
    e.getClassName == s"$scioNs.values.SCollectionImpl" && e.getMethodName == "transform"

  private def isPCollectionApply(e: StackTraceElement): Boolean =
    e.getClassName == s"$beamNs.sdk.values.PCollection" && e.getMethodName == "apply"

  def getAppName: String = {
    Thread
      .currentThread()
      .getStackTrace
      .drop(1)
      .map(_.getClassName)
      .find(isExternalClass)
      .getOrElse("unknown")
      .split("\\.")
      .last
      .replaceAll("\\$$", "")
  }

  def getCurrent: String = {
    val (method, location, nested) = getCurrentName
    val idx = nameCache.merge((method, location, nested), 1, new BiFunction[Int, Int, Int] {
      override def apply(t: Int, u: Int): Int = t + u
    })

    if (nested) {
      s"$method:$idx"
    } else {
      s"$method@{$location}:$idx"
    }
  }

  /** Get current call site name in the form of "method@{file:line}". */
  def getCurrentName: (String, String, Boolean) = {
    val stack = Thread.currentThread().getStackTrace.drop(1)
    val firstExtIdx = stack.indexWhere(e => isExternalClass(e.getClassName))
    val scioMethod = stack(firstExtIdx - 1).getMethodName
    val method = methodMap.getOrElse(scioMethod, scioMethod)

    // find first stack outside of Scio or SDK
    val externalCall = stack(firstExtIdx)
    val location = s"${externalCall.getFileName}:${externalCall.getLineNumber}"
    // check if transform is nested
    val transformIdx = stack.indexWhere(isTransform)
    val pApplyIdx = stack.indexWhere(isPCollectionApply)
    val isNested = transformIdx > 0 && pApplyIdx > 0
    val collIdx = stack.lastIndexWhere(_.getClassName.contains("SCollectionImpl"), pApplyIdx)

    (stack.lift(collIdx).fold(method)(_.getMethodName), location, isNested)
  }
}
