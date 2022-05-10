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

object Scalac {

  private val parallelism = math.min(Runtime.getRuntime.availableProcessors(), 16)
  val parallelismOption = new ScalacOption(
    "-Ybackend-parallelism" :: parallelism.toString :: Nil,
    version => version.isBetween(V2_12_0, V3_0_0)
  )

  // Set the strategy used for translating lambdas into JVM code to "inline"
  val delambdafyInlineOption = new ScalacOption(
    "-Ydelambdafy:inline" :: Nil
  )

  val macroAnnotationsOption = new ScalacOption(
    "-Ymacro-annotations" :: Nil,
    version => version.isBetween(V2_13_0, V3_0_0)
  )

  val macroSettingsOption = new ScalacOption(
    "-Xmacro-settings:show-coder-fallback=true" :: Nil
  )

  val maxClassfileName = new ScalacOption(
    "-Xmax-classfile-name" :: "100" :: Nil,
    version => version.isBetween(V2_12_0, V2_13_0)
  )

  // JVM
  val targetOption = new ScalacOption("-target:jvm-1.8" :: Nil)

  private val javaVersion = VersionNumber(sys.props("java.version"))
  val releaseOption = new ScalacOption(
    "-release" :: "8" :: Nil,
    _ => javaVersion.matchesSemVer(SemanticSelector(">1.8"))
  )

  // Warn
  val privateWarnMacrosOption = new ScalacOption(
    "-Ywarn-macros:after" :: Nil,
    version => version.isBetween(V2_12_0, V2_13_0)
  )
  val warnMacrosOption = new ScalacOption(
    "-Wmacros:after" :: Nil,
    version => version.isBetween(V2_13_0, V3_0_0)
  )
  // silence all scala library deprecation warnings in 2.13
  // since we still support 2.12
  // unused-imports origin will be supported in next 2.13.9
  // https://github.com/scala/scala/pull/9939
  val warnConfOption = new ScalacOption(
    "-Wconf:cat=deprecation&origin=scala\\..*&since>2.12.99:s" +
      ",cat=unused-imports&origin=scala\\.collection\\.compat\\..*:s"
      :: Nil,
    version => version.isBetween(V2_13_2, V3_0_0)
  )

  // Doc
  val docNoJavaCommentOption = new ScalacOption(
    "-no-java-comments" :: Nil,
    version => version.isBetween(V2_12_0, V2_13_0)
  )

  val docSkipPackageOption = new ScalacOption(
    "-skip-packages" :: "org.apache" :: Nil
  )

}
