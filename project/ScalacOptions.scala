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

import sbt._, Keys._
import sbt.librarymanagement.{SemanticSelector, VersionNumber}
import java.lang.Runtime
import _root_.io.github.davidgregory084.ScalacOption
import _root_.io.github.davidgregory084.ScalaVersion._

object Scalac {

  val macroAnnotationsOption = new ScalacOption(
    "-Ymacro-annotations" :: Nil,
    version => version.isBetween(V2_13_0, V3_0_0)
  )

  private val parallelism = math.min(Runtime.getRuntime.availableProcessors(), 16)
  val parallelismOption = new ScalacOption(
    "-Ybackend-parallelism" :: parallelism.toString :: Nil,
    version => version.isBetween(V2_12_0, V3_0_0)
  )

  val targetOption = new ScalacOption("-target:jvm-1.8" :: Nil)

  private val javaVersion = VersionNumber(sys.props("java.version"))
  val releaseOption = new ScalacOption(
    "-release" :: "8" :: Nil,
    _ => javaVersion.matchesSemVer(SemanticSelector(">1.8"))
  )

  val warnMacrosOption = new ScalacOption(
    "-Ywarn-macros:after" :: Nil,
    version => version.isBetween(V2_12_0, V3_0_0)
  )
}
