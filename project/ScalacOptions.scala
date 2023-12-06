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
import sbt._
object ScalacOptions {

  import org.typelevel.scalacoptions.ScalacOption
  import org.typelevel.scalacoptions.ScalaVersion
  import org.typelevel.scalacoptions.ScalaVersion._
  import org.typelevel.scalacoptions.ScalacOptions._

  // Set the strategy used for translating lambdas into JVM code to "inline"
  val delambdafyInlineOption = privateOption("delambdafy:inline")

  val macroAnnotationsOption = privateOption(
    "macro-annotations",
    _.isBetween(V2_13_0, V3_0_0)
  )

  def macroShowCoderFallback(enabled: Boolean) =
    advancedOption(s"macro-settings:show-coder-fallback=$enabled")

  def macroCacheImplicitSchemas(enabled: Boolean) =
    advancedOption(s"macro-settings:cache-implicit-schemas=$enabled")
  def maxClassfileName(limit: Int) = advancedOption(
    "max-classfile-name",
    List(limit.toString),
    _.isBetween(V2_12_0, V2_13_0)
  )

  private val cpuParallelism = math.min(java.lang.Runtime.getRuntime.availableProcessors(), 16)

  // Warn
  val privateWarnMacrosOption = privateWarnOption(
    "macros:after",
    _.isBetween(V2_12_0, V2_13_0)
  )
  val warnMacrosOption = warnOption(
    "macros:after",
    _.isBetween(V2_13_0, V3_0_0)
  )
  // silence all scala library deprecation warnings in 2.13
  // since we still support 2.12
  // unused-imports origin will be supported in next 2.13.9
  // https://github.com/scala/scala/pull/9939
  val warnConfOption = warnOption(
    "conf:cat=deprecation&origin=scala\\..*&since>2.12.99:s" +
      ",cat=unused-imports&origin=scala\\.collection\\.compat\\..*:s",
    _.isBetween(V2_13_2, V3_0_0)
  )

  // Doc
  val docNoJavaCommentOption = other(
    "-no-java-comments",
    _.isBetween(V2_12_0, V2_13_0)
  )

  def tokensForVersion(
    scalaVersion: String,
    proposedScalacOptions: Set[ScalacOption]
  ): Seq[String] = {
    val Seq(major, minor, patch) = VersionNumber(scalaVersion).numbers
    org.typelevel.scalacoptions.ScalacOptions
      .tokensForVersion(ScalaVersion(major, minor, patch), proposedScalacOptions)
  }

  def defaults(scalaVersion: String): Seq[String] = tokensForVersion(
    scalaVersion,
    Set(
      delambdafyInlineOption,
      macroAnnotationsOption,
      macroShowCoderFallback(true),
      maxClassfileName(100),
      privateBackendParallelism(cpuParallelism),
      privateWarnMacrosOption,
      warnMacrosOption,
      warnConfOption
    )
  )
}
