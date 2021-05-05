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

object Scalac {
  // see: https://tpolecat.github.io/2017/04/25/scalac-flags.html
  val parallelism = math.min(math.max(4, java.lang.Runtime.getRuntime().availableProcessors()), 16)
  val baseOptions = Def.setting {
    List(
      "-target:jvm-1.8",
      "-deprecation", // Emit warning and location for usages of deprecated APIs.
      "-feature", // Emit warning and location for usages of features that should be imported explicitly.
      "-unchecked", // Enable additional warnings where generated code depends on assumptions.
      "-encoding",
      "utf-8", // Specify character encoding used by source files.
      "-explaintypes", // Explain type errors in more detail.
      // "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
      "-language:experimental.macros", // Allow macro definition (besides implementation and application)
      "-language:higherKinds", // Allow higher-kinded types
      "-language:implicitConversions", // Allow definition of implicit functions called views
      "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
      // "-Xfatal-warnings", // Fail the compilation if there are any warnings.
      // "-Xfuture", // Turn on future language features.
      "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
      // "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
      "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
      "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
      "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
      "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
      "-Xlint:option-implicit", // Option.apply used implicit view.
      // "-Xlint:package-object-classes", // Class or object defined in package object.
      "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
      "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
      "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
      "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
      // "-Ywarn-dead-code", // Warn when dead code is identified.
      // "-Ywarn-numeric-widen", // Warn when numerics are widened.
      // "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
      // "-Ywarn-unused:locals", // Warn if a local definition is unused.
      // "-Ywarn-unused:params", // Warn if a value parameter is unused.
      // "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
      // "-Ywarn-unused:privates", // Warn if a private member is unused.
      "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
      "-Xmacro-settings:show-coder-fallback=true",
      "-Ydelambdafy:inline" // Set the strategy used for translating lambdas into JVM code to "inline"
      // "-Ybackend-parallelism",
      // parallelism.toString
    )
  }

  val scalaVersionOptions = Def.setting {
    VersionNumber(scalaVersion.value) match {
      case v if v.matchesSemVer(SemanticSelector("2.12.x")) =>
        List(
          "-Xmax-classfile-name",
          "100",
          "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
          "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
          "-Xlint:unsound-match", // Pattern match may not be typesafe.
          "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
          "-Ypartial-unification", // Enable partial unification in type constructor inference
          "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
          "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.,
          "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
          "-Ywarn-infer-any" // Warn when a type argument is inferred to be `Any`.
        )
      case v if v.matchesSemVer(SemanticSelector(">=2.13")) =>
        List("-Ymacro-annotations", "-Ywarn-unused")
      case _ =>
        Nil
    }
  }

  val jvmVersionOptions = Def.setting {
    VersionNumber(sys.props("java.version")) match {
      case v if v.matchesSemVer(SemanticSelector(">1.8")) => List("-release", "8")
      case _                                              => Nil
    }
  }

  val commonsOptions =
    Def.setting(baseOptions.value ++ scalaVersionOptions.value ++ jvmVersionOptions.value)

  val docOptions = Def.setting {
    val base =
      baseOptions.value ++ scalaVersionOptions.value ++ List("-skip-packages", "org.apache")

    VersionNumber(scalaVersion.value) match {
      case v if v.matchesSemVer(SemanticSelector("2.12.x")) => base ++ List("-no-java-comments")
      case _                                                => base
    }
  }

  val replOptions = Def.setting(baseOptions.value ++ scalaVersionOptions.value)

}
