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

import sbt.librarymanagement.{SemanticSelector, VersionNumber}
import java.lang.Runtime
import _root_.io.github.davidgregory084.ScalacOption
import _root_.io.github.davidgregory084.ScalaVersion._
import _root_.io.github.davidgregory084.TpolecatPlugin.autoImport.ScalacOptions

object Scalac {

  // Set the strategy used for translating lambdas into JVM code to "inline"
  val delambdafyInlineOption = ScalacOptions.privateOption("delambdafy:inline")

  val macroAnnotationsOption = ScalacOptions.privateOption(
    "macro-annotations",
    _.isBetween(V2_13_0, V3_0_0)
  )

  val macroSettingsOption = ScalacOptions.advancedOption("macro-settings:show-coder-fallback=true")

  val maxClassfileName = ScalacOptions.advancedOption(
    "max-classfile-name",
    List("100"),
    _.isBetween(V2_12_0, V2_13_0)
  )

  private val parallelism = math.min(java.lang.Runtime.getRuntime.availableProcessors(), 16)
  val privateBackendParallelism = ScalacOptions.privateBackendParallelism(parallelism)

  val release8 = ScalacOptions.release("8")

  // JVM
  val targetOption = ScalacOptions.other("-target:jvm-1.8")

  // Warn
  val privateWarnMacrosOption = ScalacOptions.privateWarnOption(
    "macros:after",
    _.isBetween(V2_12_0, V2_13_0)
  )
  val warnMacrosOption = ScalacOptions.warnOption(
    "macros:after",
    _.isBetween(V2_13_0, V3_0_0)
  )
  // silence all scala library deprecation warnings in 2.13
  // since we still support 2.12
  // unused-imports origin will be supported in next 2.13.9
  // https://github.com/scala/scala/pull/9939
  val warnConfOption = ScalacOptions.warnOption(
    "conf:cat=deprecation&origin=scala\\..*&since>2.12.99:s" +
      ",cat=unused-imports&origin=scala\\.collection\\.compat\\..*:s",
    _.isBetween(V2_13_2, V3_0_0)
  )

  // Doc
  val docNoJavaCommentOption = ScalacOptions.other(
    "-no-java-comments",
    _.isBetween(V2_12_0, V2_13_0)
  )
}
