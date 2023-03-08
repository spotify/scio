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

import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import com.github.sbt.git.SbtGit.GitKeys.gitRemoteRepo
import com.typesafe.tools.mima.core._
import org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings
import bloop.integrations.sbt.BloopDefaults
import de.heikoseeberger.sbtheader.CommentCreator
import _root_.io.github.davidgregory084.DevMode

ThisBuild / turbo := true

val beamVendorVersion = "0.1"
val beamVersion = "2.45.0"

// check version used by beam
// https://github.com/apache/beam/blob/v2.45.0/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
val autoServiceVersion = "1.0.1"
val autoValueVersion = "1.9"
val avroVersion = "1.8.2"
val bigdataossVersion = "2.2.6"
val bigtableClientVersion = "1.26.3"
val commonsCodecVersion = "1.15"
val commonsCompressVersion = "1.21"
val datastoreV1ProtoClientVersion = "2.9.0"
val flinkVersion = "1.15.0"
val googleClientsVersion = "2.0.0"
val googleOauthClientVersion = "1.34.1"
val guavaVersion = "31.1-jre"
val hadoopVersion = "2.10.2"
val httpCoreVersion = "4.4.14"
val jacksonVersion = "2.14.1"
val javaxAnnotationApiVersion = "1.3.2"
val nettyTcNativeVersion = "2.0.52.Final"
val nettyVersion = "4.1.77.Final"
val slf4jVersion = "1.7.30"
val sparkVersion = "3.1.2"
// dependent versions
val googleApiServicesBigQueryVersion = s"v2-rev20220924-$googleClientsVersion"
val googleApiServicesDataflowVersion = s"v1b3-rev20220920-$googleClientsVersion"
val googleApiServicesPubsubVersion = s"v1-rev20220904-$googleClientsVersion"
val googleApiServicesStorageVersion = s"v1-rev20220705-$googleClientsVersion"

// check versions from libraries-bom
// https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/26.5.0/index.html
val animalSnifferAnnotationsVersion = "1.22"
val bigQueryStorageBetaVersion = "0.152.4"
val bigQueryStorageVersion = "2.28.4"
val checkerFrameworkVersion = "3.29.0"
val errorProneAnnotationsVersion = "2.18.0"
val floggerVersion = "0.7.4"
val gaxHttpJsonVersion = "0.107.0"
val gaxVersion = "2.22.0"
val googleApiCommonVersion = "2.5.0"
val googleAuthVersion = "1.14.0"
val googleCloudBigTableVersion = "2.18.3"
val googleCloudCoreVersion = "2.9.4"
val googleCloudDatastoreVersion = "0.104.3"
val googleCloudMonitoringVersion = "3.10.0"
val googleCloudSpannerVersion = "6.35.2"
val googleCloudStorageVersion = "2.17.2"
val googleCommonsProtoVersion = "2.13.0"
val googleHttpClientsVersion = "1.42.3"
val googleIAMVersion = "1.8.0"
val grpcVersion = "1.52.1"
val opencensusVersion = "0.31.1"
val perfmarkVersion = "0.26.0"
val protobufVersion = "3.21.12"

val algebirdVersion = "0.13.9"
val algebraVersion = "2.9.0"
val annoy4sVersion = "0.10.0"
val annoyVersion = "0.2.6"
val breezeVersion = "2.1.0"
val caffeineVersion = "2.9.3"
val cassandraDriverVersion = "3.11.3"
val cassandraVersion = "3.11.13"
val catsVersion = "2.9.0"
val chillVersion = "0.10.0"
val circeVersion = "0.14.4"
val commonsIoVersion = "2.11.0"
val commonsLang3Version = "3.12.0"
val commonsMath3Version = "3.6.1"
val commonsTextVersion = "1.10.0"
val elasticsearch6Version = "6.8.23"
val elasticsearch7Version = "7.17.8"
val elasticsearch8Version = "8.6.2"
val featranVersion = "0.8.0"
val hamcrestVersion = "2.2"
val javaLshVersion = "0.12"
val jodaTimeVersion = "2.12.2"
val junitInterfaceVersion = "0.13.3"
val junitVersion = "4.13.2"
val kantanCodecsVersion = "0.5.1"
val kantanCsvVersion = "0.7.0"
val kryoVersion = "4.0.2"
val magnoliaVersion = "1.1.3"
val magnolifyVersion = "0.6.2"
val metricsVersion = "3.2.6"
val neo4jDriverVersion = "4.4.11"
val parquetExtraVersion = "0.4.3"
val parquetVersion = "1.12.3"
val pprintVersion = "0.8.1"
val protobufGenericVersion = "0.2.9"
val scalacheckVersion = "1.17.0"
val scalaCollectionCompatVersion = "2.9.0"
val scalacticVersion = "3.2.15"
val scalaMacrosVersion = "2.1.1"
val scalatestVersion = "3.2.15"
val shapelessVersion = "2.3.10"
val sparkeyVersion = "3.2.5"
val tensorFlowVersion = "0.4.2"
val testContainersVersion = "0.40.12"
val zoltarVersion = "0.6.0"
// dependent versions
val scalatestplusVersion = s"$scalatestVersion.0"

ThisBuild / tpolecatDefaultOptionsMode := DevMode
ThisBuild / tpolecatDevModeOptions ~= { opts =>
  val excludes = Set(
    ScalacOptions.lintPackageObjectClasses,
    ScalacOptions.privateWarnDeadCode,
    ScalacOptions.privateWarnValueDiscard,
    ScalacOptions.warnDeadCode,
    ScalacOptions.warnValueDiscard
  )

  val extras = Set(
    Scalac.delambdafyInlineOption,
    Scalac.macroAnnotationsOption,
    Scalac.macroSettingsOption,
    Scalac.maxClassfileName,
    Scalac.privateBackendParallelism,
    Scalac.privateWarnMacrosOption,
    Scalac.release8,
    Scalac.targetOption,
    Scalac.warnConfOption,
    Scalac.warnMacrosOption
  )

  opts.filterNot(excludes).union(extras)
}

ThisBuild / doc / tpolecatDevModeOptions ++= Set(
  Scalac.docNoJavaCommentOption
)

ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value)
val excludeLint = SettingKey[Set[Def.KeyedInitialize[_]]]("excludeLintKeys")
Global / excludeLint := (Global / excludeLint).?.value.getOrElse(Set.empty)
Global / excludeLint += sonatypeProfileName
Global / excludeLint += site / Paradox / sourceManaged

def previousVersion(currentVersion: String): Option[String] = {
  val Version =
    """(?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)(?<preRelease>-.*)?(?<build>\+.*)?""".r
  currentVersion match {
    case Version(x, y, z, null, null) if z != "0" =>
      // patch release
      Some(s"$x.$y.${z.toInt - 1}")
    case Version(x, y, z, null, _) =>
      // post release build
      Some(s"$x.$y.$z")
    case Version(x, y, z, _, _) if z != "0" =>
      // patch pre-release
      Some(s"$x.$y.${z.toInt - 1}")
    case _ =>
      None
  }
}

lazy val mimaSettings = Def.settings(
  mimaBinaryIssueFilters := Seq(
    // minor scio-tensorflow breaking changes for 0.12.6
    ProblemFilters.exclude[DirectMissingMethodProblem](
      "com.spotify.scio.tensorflow.syntax.SeqExampleSCollectionOps.saveAsTfRecordFile$extension"
    ),
    ProblemFilters.exclude[DirectMissingMethodProblem](
      "com.spotify.scio.tensorflow.syntax.SeqExampleSCollectionOps.saveAsTfRecordFile"
    ),
    ProblemFilters.exclude[DirectMissingMethodProblem](
      "com.spotify.scio.tensorflow.syntax.SeqExampleSCollectionOps.saveAsTfRecordFile$extension"
    )
  ),
  mimaPreviousArtifacts :=
    previousVersion(version.value)
      .filter(_ => publishArtifact.value)
      .map(organization.value % s"${normalizedName.value}_${scalaBinaryVersion.value}" % _)
      .toSet
)

lazy val formatSettings = Def.settings(scalafmtOnCompile := false, javafmtOnCompile := false)

lazy val currentYear = java.time.LocalDate.now().getYear
lazy val keepExistingHeader =
  HeaderCommentStyle.cStyleBlockComment.copy(commentCreator = new CommentCreator() {
    override def apply(text: String, existingText: Option[String]): String =
      existingText
        .getOrElse(
          HeaderCommentStyle.cStyleBlockComment.commentCreator(text)
        )
        .trim()
  })

val commonSettings = Def
  .settings(
    organization := "com.spotify",
    headerLicense := Some(HeaderLicense.ALv2(currentYear.toString, "Spotify AB")),
    headerMappings := headerMappings.value + (HeaderFileType.scala -> keepExistingHeader, HeaderFileType.java -> keepExistingHeader),
    scalaVersion := "2.13.8",
    crossScalaVersions := Seq("2.12.17", scalaVersion.value),
    // this setting is not derived in sbt-tpolecat
    // https://github.com/typelevel/sbt-tpolecat/issues/36
    inTask(doc)(TpolecatPlugin.projectSettings),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
    Compile / doc / javacOptions := Seq("-source", "1.8"),
    excludeDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-io-kafka"
    ),
    resolvers ++= Resolver.sonatypeOssRepos("public"),
    Test / javaOptions += "-Dscio.ignoreVersionWarning=true",
    Test / testOptions += Tests.Argument("-oD"),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-a"),
    testOptions ++= {
      if (sys.env.contains("SLOW")) {
        Nil
      } else {
        Seq(Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow"))
      }
    },
    coverageExcludedPackages := (Seq(
      "com\\.spotify\\.scio\\.examples\\..*",
      "com\\.spotify\\.scio\\.repl\\..*",
      "com\\.spotify\\.scio\\.util\\.MultiJoin",
      "com\\.spotify\\.scio\\.smb\\.util\\.SMBMultiJoin"
    ) ++ (2 to 10).map(x => s"com\\.spotify\\.scio\\.sql\\.Query$x")).mkString(";"),
    coverageHighlighting := true,
    licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage := Some(url("https://github.com/spotify/scio")),
    scmInfo := Some(
      ScmInfo(url("https://github.com/spotify/scio"), "scm:git:git@github.com:spotify/scio.git")
    ),
    developers := List(
      Developer(
        id = "sinisa_lyh",
        name = "Neville Li",
        email = "neville.lyh@gmail.com",
        url = url("https://twitter.com/sinisa_lyh")
      ),
      Developer(
        id = "ravwojdyla",
        name = "Rafal Wojdyla",
        email = "ravwojdyla@gmail.com",
        url = url("https://twitter.com/ravwojdyla")
      ),
      Developer(
        id = "andrewsmartin",
        name = "Andrew Martin",
        email = "andrewsmartin.mg@gmail.com",
        url = url("https://twitter.com/andrew_martin92")
      ),
      Developer(
        id = "fallonfofallon",
        name = "Fallon Chen",
        email = "fallon@spotify.com",
        url = url("https://twitter.com/fallonfofallon")
      ),
      Developer(
        id = "regadas",
        name = "Filipe Regadas",
        email = "filiperegadas@gmail.com",
        url = url("https://twitter.com/regadas")
      ),
      Developer(
        id = "jto",
        name = "Julien Tournay",
        email = "julient@spotify.com",
        url = url("https://twitter.com/skaalf")
      ),
      Developer(
        id = "clairemcginty",
        name = "Claire McGinty",
        email = "clairem@spotify.com",
        url = url("http://github.com/clairemcginty")
      ),
      Developer(
        id = "syodage",
        name = "Shameera Rathnayaka",
        email = "shameerayodage@gmail.com",
        url = url("http://github.com/syodage")
      ),
      Developer(
        id = "kellen",
        name = "Kellen Dye",
        email = "dye.kellen@gmail.com",
        url = url("http://github.com/kellen")
      ),
      Developer(
        id = "farzad-sedghi",
        name = "farzad sedghi",
        email = "farzadsedghi2@gmail.com",
        url = url("http://github.com/farzad-sedghi")
      )
    ),
    mimaSettings,
    formatSettings,
    java17Settings
  )

lazy val publishSettings = Def.settings(
  // Release settings
  sonatypeProfileName := "com.spotify"
)

lazy val itSettings = Defaults.itSettings ++ inConfig(IntegrationTest)(
  Def.settings(
    classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    // exclude all sources if we don't have GCP credentials
    unmanagedSources / excludeFilter := {
      if (BuildCredentials.exists) {
        HiddenFileFilter
      } else {
        HiddenFileFilter || "*.scala"
      }
    },
    run / fork := true,
    BloopDefaults.configSettings,
    scalafmtConfigSettings,
    scalafixConfigSettings(IntegrationTest)
  )
)

lazy val assemblySettings = Seq(
  assembly / test := {},
  assembly / assemblyMergeStrategy ~= { old =>
    {
      case PathList("dev", "ludovic", "netlib", "InstanceBuilder.class") =>
        // arbitrary pick last conflicting InstanceBuilder
        MergeStrategy.last
      case s if s.endsWith(".proto") =>
        // arbitrary pick last conflicting proto file
        MergeStrategy.last
      case PathList("git.properties") =>
        // drop conflicting git properties
        MergeStrategy.discard
      case PathList("META-INF", "versions", "9", "module-info.class") =>
        // drop conflicting module-info.class
        MergeStrategy.discard
      case PathList("META-INF", "gradle", "incremental.annotation.processors") =>
        // drop conflicting kotlin compiler info
        MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") =>
        // merge conflicting netty property files
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "native-image", "native-image.properties") =>
        // merge conflicting native-image property files
        MergeStrategy.filterDistinctLines
      case s => old(s)
    }
  }
)

lazy val macroSettings = Def.settings(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  libraryDependencies ++= {
    VersionNumber(scalaVersion.value) match {
      case v if v.matchesSemVer(SemanticSelector("2.12.x")) =>
        Seq(
          compilerPlugin(
            ("org.scalamacros" % "paradise" % scalaMacrosVersion).cross(CrossVersion.full)
          )
        )
      case _ => Nil
    }
  },
  // see MacroSettings.scala
  scalacOptions += "-Xmacro-settings:cache-implicit-schemas=true"
)

lazy val directRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime
)
lazy val dataflowRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime
)

// only available for scala 2.12
// scala 2.13 is supported from spark 3.2.0
lazy val sparkRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-spark" % beamVersion % Runtime,
  "org.apache.spark" %% "spark-core" % sparkVersion % Runtime,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Runtime
)

lazy val flinkRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-flink-1.15" % beamVersion % Runtime,
  "org.apache.flink" % "flink-clients" % flinkVersion % Runtime,
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % Runtime
)
lazy val beamRunners = settingKey[String]("beam runners")
lazy val beamRunnersEval = settingKey[Seq[ModuleID]]("beam runners")

def beamRunnerSettings: Seq[Setting[_]] = Seq(
  beamRunners := "",
  beamRunnersEval := {
    Option(beamRunners.value)
      .filter(_.nonEmpty)
      .orElse(sys.props.get("beamRunners"))
      .orElse(sys.env.get("BEAM_RUNNERS"))
      .map(_.split(","))
      .map {
        _.flatMap {
          case "DirectRunner"   => directRunnerDependencies
          case "DataflowRunner" => dataflowRunnerDependencies
          case "SparkRunner"    => sparkRunnerDependencies
          case "FlinkRunner"    => flinkRunnerDependencies
          case _                => Nil
        }.toSeq
      }
      .getOrElse(directRunnerDependencies)
  },
  libraryDependencies ++= beamRunnersEval.value
)

ThisBuild / PB.protocVersion := protobufVersion
lazy val scopedProtobufSettings = Def.settings(
  PB.targets := Seq(
    PB.gens.java -> (ThisScope.copy(config = Zero) / sourceManaged).value /
      "compiled_proto" /
      configuration.value.name,
    PB.gens.plugin("grpc-java") -> (ThisScope.copy(config = Zero) / sourceManaged).value /
      "compiled_grpc" /
      configuration.value.name
  ),
  managedSourceDirectories ++= PB.targets.value.map(_.outputPath)
)

lazy val protobufSettings = Def.settings(
  libraryDependencies ++= Seq(
    "io.grpc" % "protoc-gen-grpc-java" % grpcVersion asProtocPlugin (),
    "com.google.protobuf" % "protobuf-java" % protobufVersion % "protobuf"
  )
) ++ Seq(Compile, Test).flatMap(c => inConfig(c)(scopedProtobufSettings))

def splitTests(tests: Seq[TestDefinition], filter: Seq[String], forkOptions: ForkOptions) = {
  val (filtered, default) = tests.partition(test => filter.contains(test.name))
  val policy = Tests.SubProcess(forkOptions)
  new Tests.Group(name = "<default>", tests = default, runPolicy = policy) +: filtered.map { test =>
    new Tests.Group(name = test.name, tests = Seq(test), runPolicy = policy)
  }
}

lazy val java17Settings = sys.props("java.version") match {
  case v if v.startsWith("17.") =>
    Seq(
      Test / fork := true,
      Test / javaOptions ++= Seq(
        "--add-opens",
        "java.base/java.util=ALL-UNNAMED",
        "--add-opens",
        "java.base/java.lang.invoke=ALL-UNNAMED"
      )
    )
  case _ => Seq()
}

lazy val root: Project = Project("scio", file("."))
  .settings(commonSettings)
  .settings(
    publish / skip := true,
    mimaPreviousArtifacts := Set.empty,
    assembly / aggregate := false
  )
  .aggregate(
    `scio-avro`,
    `scio-cassandra3`,
    `scio-core`,
    `scio-elasticsearch6`,
    `scio-elasticsearch7`,
    `scio-elasticsearch8`,
    `scio-examples`,
    `scio-extra`,
    `scio-google-cloud-platform`,
    `scio-grpc`,
    `scio-jdbc`,
    `scio-jmh`,
    `scio-macros`,
    `scio-neo4j`,
    `scio-parquet`,
    `scio-redis`,
    `scio-repl`,
    `scio-smb`,
    `scio-tensorflow`,
    `scio-test`
  )

lazy val `scio-core`: Project = project
  .in(file("scio-core"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(`scio-macros`)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(
    description := "Scio - A Scala API for Apache Beam and Google Cloud Dataflow",
    Compile / resources ++= Seq(
      (ThisBuild / baseDirectory).value / "build.sbt",
      (ThisBuild / baseDirectory).value / "version.sbt"
    ),
    libraryDependencies ++= Seq(
      // compile
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.google.api-client" % "google-api-client" % googleClientsVersion,
      "com.google.apis" % "google-api-services-dataflow" % googleApiServicesDataflowVersion,
      "com.google.auto.service" % "auto-service" % autoServiceVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
      "com.google.http-client" % "google-http-client-jackson2" % googleHttpClientsVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" % "chill-protobuf" % chillVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" %% "chill-algebird" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "io.grpc" % "grpc-api" % grpcVersion,
      "io.grpc" % "grpc-auth" % grpcVersion,
      "io.grpc" % "grpc-core" % grpcVersion,
      "io.grpc" % "grpc-netty" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion,
      "io.netty" % "netty-handler" % nettyVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-runners-core-construction-java" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-protobuf" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.apache.commons" % "commons-compress" % commonsCompressVersion,
      "org.apache.commons" % "commons-lang3" % commonsLang3Version,
      "org.apache.commons" % "commons-math3" % commonsMath3Version,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.typelevel" %% "algebra" % algebraVersion,
      // provided
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % Provided,
      "org.apache.beam" % "beam-runners-flink-1.15" % beamVersion % Provided,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Provided,
      "org.apache.beam" % "beam-runners-spark" % beamVersion % Provided,
      // test
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),
    buildInfoKeys := Seq[BuildInfoKey](scalaVersion, version, "beamVersion" -> beamVersion),
    buildInfoPackage := "com.spotify.scio"
  )

lazy val `scio-test`: Project = project
  .in(file("scio-test"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(macroSettings)
  .settings(protobufSettings)
  .settings(
    description := "Scio helpers for ScalaTest",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.github.sbt" % "junit-interface" % junitInterfaceVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % googleCloudBigTableVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.lihaoyi" %% "pprint" % pprintVersion,
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "com.spotify" %% "magnolify-guava" % magnolifyVersion,
      "com.twitter" %% "chill" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "junit" % "junit" % junitVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.hamcrest" % "hamcrest" % hamcrestVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.scalactic" %% "scalactic" % scalacticVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion,
      // runtime
      // https://github.com/sbt/sbt/issues/5568
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion, // % Runtime,
      // test
      "com.spotify" % "annoy" % annoyVersion % "test",
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion % "it",
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion % "test",
      // Our BloomFilters are Algebird Monoids and hence uses tests from Algebird Test
      "com.twitter" %% "algebird-test" % algebirdVersion % "test",
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % "test,it",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test" classifier "tests",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "org.scalatestplus" %% "scalacheck-1-17" % scalatestplusVersion % "test,it"
    ),
    Test / compileOrder := CompileOrder.JavaThenScala,
    Test / testGrouping := splitTests(
      (Test / definedTests).value,
      List("com.spotify.scio.ArgsTest"),
      (Test / forkOptions).value
    )
  )
  .configs(IntegrationTest)
  .dependsOn(
    `scio-core` % "test->test;compile->compile;it->it",
    `scio-avro` % "compile->test;it->it"
  )

lazy val `scio-macros`: Project = project
  .in(file("scio-macros"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio macros",
    libraryDependencies ++= Seq(
      // compile
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion
    )
  )

lazy val `scio-avro`: Project = project
  .in(file("scio-avro"))
  .dependsOn(
    `scio-core` % "compile;it->it"
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for working with Avro",
    libraryDependencies ++= Seq(
      // compile
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.twitter" %% "chill" % chillVersion,
      "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
      "org.apache.avro" % "avro" % avroVersion excludeAll (
        "com.thoughtworks.paranamer" % "paranamer"
      ),
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // test
      "com.spotify" %% "magnolify-cats" % magnolifyVersion % "test",
      "com.spotify" %% "magnolify-scalacheck" % magnolifyVersion % "test",
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % "test,it",
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion % "it",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "org.scalatestplus" %% "scalacheck-1-17" % scalatestplusVersion % "test,it",
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
      "org.typelevel" %% "cats-core" % catsVersion % "test"
    )
  )

lazy val `scio-google-cloud-platform`: Project = project
  .in(file("scio-google-cloud-platform"))
  .dependsOn(
    `scio-core` % "compile;it->it",
    `scio-avro` % "test",
    `scio-test` % "test;it"
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Scio add-on for Google Cloud Platform",
    libraryDependencies ++= Seq(
      // compile
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.google.api" % "gax" % gaxVersion,
      "com.google.api" % "gax-grpc" % gaxVersion,
      "com.google.api-client" % "google-api-client" % googleClientsVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigquerystorage-v1" % bigQueryStorageVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigquerystorage-v1beta2" % bigQueryStorageBetaVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-admin-v2" % googleCloudBigTableVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % googleCloudBigTableVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.google.auth" % "google-auth-library-credentials" % googleAuthVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
      "com.google.cloud" % "google-cloud-bigquerystorage" % bigQueryStorageVersion,
      "com.google.cloud" % "google-cloud-core" % googleCloudCoreVersion,
      "com.google.cloud" % "google-cloud-spanner" % googleCloudSpannerVersion,
      "com.google.cloud.bigtable" % "bigtable-client-core" % bigtableClientVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
      "com.google.http-client" % "google-http-client-jackson2" % googleHttpClientsVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.twitter" %% "chill" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion excludeAll (
        // remove transitive test dependencies at compile time
        "junit" % "junit",
        "org.hamcrest" % "hamcrest-core"
      ),
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // test
      "com.spotify" %% "magnolify-cats" % magnolifyVersion % "test",
      "org.typelevel" %% "cats-core" % catsVersion % "test",
      "com.spotify" %% "magnolify-scalacheck" % magnolifyVersion % "test",
      "junit" % "junit" % junitVersion % "test",
      "com.google.cloud" % "google-cloud-storage" % googleCloudStorageVersion % "test,it",
      "org.hamcrest" % "hamcrest" % hamcrestVersion % "test,it",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "org.scalatestplus" %% "scalacheck-1-17" % scalatestplusVersion % "test,it",
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it"
    )
  )

lazy val `scio-cassandra3`: Project = project
  .in(file("scio-cassandra/cassandra3"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test;it"
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for Apache Cassandra 3.x",
    libraryDependencies ++= Seq(
      // compile
      "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "chill" % chillVersion,
      "org.apache.cassandra" % "cassandra-all" % cassandraVersion excludeAll (
        "ch.qos.logback" % "logback-classic",
        "org.slf4j" % "log4j-over-slf4j"
      ),
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      // test
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % Test
    )
  )

lazy val `scio-elasticsearch6`: Project = project
  .in(file("scio-elasticsearch/es6"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      // compile
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch6Version,
      "org.elasticsearch" % "elasticsearch-x-content" % elasticsearch6Version,
      "org.elasticsearch.client" % "transport" % elasticsearch6Version,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // test
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val `scio-elasticsearch7`: Project = project
  .in(file("scio-elasticsearch/es7"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      // compile
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch7Version,
      "org.elasticsearch" % "elasticsearch-x-content" % elasticsearch7Version,
      "org.elasticsearch.client" % "elasticsearch-rest-client" % elasticsearch7Version,
      "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % elasticsearch7Version,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // test
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )

lazy val `scio-elasticsearch8`: Project = project
  .in(file("scio-elasticsearch/es8"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test,it"
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      // compile
      "co.elastic.clients" % "elasticsearch-java" % elasticsearch8Version,
      "co.elastic.clients" % "elasticsearch-java" % elasticsearch8Version,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // test
      "com.dimafeng" %% "testcontainers-scala-elasticsearch" % testContainersVersion % "it",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % "it",
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion % "it",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion % "it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it"
    )
  )

lazy val `scio-extra`: Project = project
  .in(file("scio-extra"))
  .dependsOn(
    `scio-core` % "compile->compile;provided->provided",
    `scio-test` % "it->it;test->test",
    `scio-avro`,
    `scio-google-cloud-platform`,
    `scio-macros`
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio extra utilities",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      "com.spotify" % "annoy" % annoyVersion,
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "info.debatty" % "java-lsh" % javaLshVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "net.pishen" %% "annoy4s" % annoy4sVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sketching" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-zetasketch" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion excludeAll (
        // remove transitive test dependencies at compile time
        "junit" % "junit",
        "org.hamcrest" % "hamcrest-core"
      ),
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.scalanlp" %% "breeze" % breezeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.typelevel" %% "algebra" % algebraVersion,
      // test
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    ),
    Compile / sourceDirectories := (Compile / sourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    Compile / managedSourceDirectories := (Compile / managedSourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    Compile / doc / sources := List(), // suppress warnings
    compileOrder := CompileOrder.JavaThenScala
  )

lazy val `scio-grpc`: Project = project
  .in(file("scio-grpc"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(protobufSettings)
  .settings(
    description := "Scio add-on for gRPC",
    libraryDependencies ++= Seq(
      // compile
      "io.grpc" % "grpc-core" % grpcVersion,
      "io.grpc" % "grpc-protobuf" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion
    )
  )

lazy val `scio-jdbc`: Project = project
  .in(file("scio-jdbc"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    description := "Scio add-on for JDBC",
    libraryDependencies ++= Seq(
      // compile
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion
    )
  )

lazy val `scio-neo4j`: Project = project
  .in(file("scio-neo4j"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test,it"
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(itSettings)
  .settings(publishSettings)
  .settings(
    description := "Scio add-on for Neo4J",
    libraryDependencies ++= Seq(
      // compile
      "com.spotify" %% "magnolify-neo4j" % magnolifyVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-neo4j" % beamVersion,
      "org.neo4j.driver" % "neo4j-java-driver" % neo4jDriverVersion,
      // test
      "com.dimafeng" %% "testcontainers-scala-neo4j" % testContainersVersion % "it",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % "it"
    )
  )

val ensureSourceManaged = taskKey[Unit]("ensureSourceManaged")

lazy val `scio-parquet`: Project = project
  .in(file("scio-parquet"))
  .dependsOn(
    `scio-core`,
    `scio-avro`,
    `scio-test` % "test->test"
  )
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    // change annotation processor output directory so IntelliJ can pick them up
    ensureSourceManaged := IO.createDirectory(sourceManaged.value / "main"),
    Compile / compile := Def.task {
      val _ = ensureSourceManaged.value
      (Compile / compile).value
    }.value,
    javacOptions ++= Seq("-s", (sourceManaged.value / "main").toString),
    description := "Scio add-on for Parquet",
    libraryDependencies ++= Seq(
      // compile
      "com.google.cloud.bigdataoss" % "gcs-connector" % s"hadoop2-$bigdataossVersion",
      "com.spotify" %% "magnolify-parquet" % magnolifyVersion,
      "com.twitter" %% "chill" % chillVersion,
      "me.lyh" % "parquet-tensorflow" % parquetExtraVersion,
      "me.lyh" %% "parquet-avro" % parquetExtraVersion excludeAll (
        // parquet-avro depends on avro 1.10.x
        "org.apache.avro" % "avro",
        "org.apache.avro" % "avro-compiler"
      ),
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.avro" % "avro-compiler" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-hadoop-common" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-hadoop-format" % beamVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion excludeAll (
        "org.apache.avro" % "avro"
      ),
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.parquet" % "parquet-common" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.tensorflow" % "tensorflow-core-api" % tensorFlowVersion
    )
  )

lazy val `scio-tensorflow`: Project = project
  .in(file("scio-tensorflow"))
  .dependsOn(
    `scio-avro`,
    `scio-core`,
    `scio-test` % "test->test"
  )
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(protobufSettings)
  .settings(
    description := "Scio add-on for TensorFlow",
    Compile / sourceDirectories := (Compile / sourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    Compile / managedSourceDirectories := (Compile / managedSourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    libraryDependencies ++= Seq(
      // compile
      "com.spotify" % "zoltar-api" % zoltarVersion,
      "com.spotify" % "zoltar-core" % zoltarVersion,
      "com.spotify" % "zoltar-tensorflow" % zoltarVersion,
      "com.spotify" %% "featran-core" % featranVersion,
      "com.spotify" %% "featran-scio" % featranVersion,
      "com.spotify" %% "featran-tensorflow" % featranVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.apache.commons" % "commons-compress" % commonsCompressVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.tensorflow" % "tensorflow-core-platform" % tensorFlowVersion,
      // test
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion % Test
    )
  )

lazy val `scio-examples`: Project = project
  .in(file("scio-examples"))
  .disablePlugins(ScalafixPlugin)
  .dependsOn(
    `scio-core`,
    `scio-google-cloud-platform`,
    `scio-jdbc`,
    `scio-extra`,
    `scio-elasticsearch8`,
    `scio-neo4j`,
    `scio-tensorflow`,
    `scio-test` % "compile->test",
    `scio-smb`,
    `scio-redis`,
    `scio-parquet`
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(soccoSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(macroSettings)
  .settings(
    publish / skip := true,
    mimaPreviousArtifacts := Set.empty,
    tpolecatExcludeOptions ++= Set(
      ScalacOptions.warnUnusedLocals,
      ScalacOptions.privateWarnUnusedLocals
    ),
    libraryDependencies ++= Seq(
      // compile
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.google.api-client" % "google-api-client" % googleClientsVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % googleCloudBigTableVersion,
      "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % googleCloudDatastoreVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.google.apis" % "google-api-services-pubsub" % googleApiServicesPubsubVersion,
      "com.google.auth" % "google-auth-library-credentials" % googleAuthVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
      "com.google.cloud.bigdataoss" % "util" % bigdataossVersion,
      "com.google.cloud.datastore" % "datastore-v1-proto-client" % datastoreV1ProtoClientVersion,
      "com.google.cloud.sql" % "mysql-socket-factory" % "1.10.0",
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
      "com.google.oauth-client" % "google-oauth-client" % googleOauthClientVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "com.spotify" %% "magnolify-avro" % magnolifyVersion,
      "com.spotify" %% "magnolify-bigtable" % magnolifyVersion,
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion,
      "com.spotify" %% "magnolify-shared" % magnolifyVersion,
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "mysql" % "mysql-connector-java" % "8.0.32",
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch7Version,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // runtime
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Runtime,
      // test
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test
    ),
    // exclude problematic sources if we don't have GCP credentials
    unmanagedSources / excludeFilter := {
      if (BuildCredentials.exists) {
        HiddenFileFilter
      } else {
        HiddenFileFilter || "TypedBigQueryTornadoes*.scala" || "TypedStorageBigQueryTornadoes*.scala"
      }
    },
    run / fork := true,
    Compile / doc / sources := List(),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    Test / testGrouping := splitTests(
      (Test / definedTests).value,
      List("com.spotify.scio.examples.WordCountTest"),
      ForkOptions().withRunJVMOptions((Test / javaOptions).value.toVector)
    )
  )

lazy val `scio-repl`: Project = project
  .in(file("scio-repl"))
  .dependsOn(
    `scio-core`,
    `scio-google-cloud-platform`,
    `scio-extra`
  )
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(macroSettings)
  .settings(
    // drop repl compatibility with java 8
    tpolecatDevModeOptions ~= { _.filterNot(_ == Scalac.release8) },
    libraryDependencies ++= Seq(
      // compile
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion excludeAll (
        "com.google.cloud.bigdataoss" % "gcsio"
      ),
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion excludeAll (
        // remove transitive test dependencies at compile time
        "junit" % "junit",
        "org.hamcrest" % "hamcrest-core"
      ),
      "org.apache.commons" % "commons-text" % commonsTextVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // runtime
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime excludeAll (
        "com.google.cloud.bigdataoss" % "gcsio",
        // remove transitive test dependencies at compile time
        "junit" % "junit",
        "org.hamcrest" % "hamcrest-core"
      ),
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Runtime
    ),
    libraryDependencies ++= {
      VersionNumber(scalaVersion.value) match {
        case v if v.matchesSemVer(SemanticSelector("2.12.x")) =>
          Seq("org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full)
        case _ =>
          Nil
      }
    },
    assembly / assemblyJarName := "scio-repl.jar"
  )

lazy val `scio-jmh`: Project = project
  .in(file("scio-jmh"))
  .enablePlugins(JmhPlugin)
  .dependsOn(
    `scio-core`,
    `scio-avro`
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio JMH Microbenchmarks",
    Jmh / sourceDirectory := (Test / sourceDirectory).value,
    Jmh / classDirectory := (Test / classDirectory).value,
    Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
    libraryDependencies ++= directRunnerDependencies ++ Seq(
      // test
      "junit" % "junit" % junitVersion % Test,
      "org.hamcrest" % "hamcrest" % hamcrestVersion % Test,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion % Test,
      "org.slf4j" % "slf4j-nop" % slf4jVersion % Test
    ),
    publish / skip := true,
    mimaPreviousArtifacts := Set.empty
  )

lazy val `scio-smb`: Project = project
  .in(file("scio-smb"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test;it",
    `scio-avro` % IntegrationTest
  )
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Sort Merge Bucket source/sink implementations for Apache Beam",
    libraryDependencies ++= Seq(
      // compile
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.google.auto.service" % "auto-service" % autoServiceVersion,
      "com.google.auto.value" % "auto-value" % autoValueVersion,
      "com.google.auto.value" % "auto-value-annotations" % autoValueVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.spotify" %% "magnolify-parquet" % magnolifyVersion,
      "javax.annotation" % "javax.annotation-api" % javaxAnnotationApiVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "it,test" classifier "tests",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-protobuf" % beamVersion,
      // #3260 work around for sorter memory limit until we patch upstream
      // "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion excludeAll (
        // remove transitive test dependencies at compile time
        "junit" % "junit",
        "org.hamcrest" % "hamcrest-core"
      ),
      "org.apache.beam" % "beam-sdks-java-io-hadoop-format" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion excludeAll (
        "org.apache.avro" % "avro"
      ),
      "org.apache.parquet" % "parquet-common" % parquetVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.tensorflow" % "tensorflow-core-platform" % tensorFlowVersion,
      // provided
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % "provided",
      // test
      "com.github.sbt" % "junit-interface" % junitInterfaceVersion % Test,
      "junit" % "junit" % junitVersion % Test,
      "org.hamcrest" % "hamcrest" % hamcrestVersion % Test
    ),
    javacOptions ++= {
      (Compile / sourceManaged).value.mkdirs()
      Seq("-s", (Compile / sourceManaged).value.getAbsolutePath)
    },
    compileOrder := CompileOrder.JavaThenScala,
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )

lazy val `scio-redis`: Project = project
  .in(file("scio-redis"))
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(
    description := "Scio integration with Redis",
    libraryDependencies ++= Seq(
      // compile
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-redis" % beamVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      // test
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val site: Project = project
  .in(file("site"))
  .enablePlugins(
    ParadoxSitePlugin,
    ParadoxMaterialThemePlugin,
    GhpagesPlugin,
    ScalaUnidocPlugin,
    SiteScaladocPlugin,
    MdocPlugin
  )
  .dependsOn(
    `scio-macros`,
    `scio-core`,
    `scio-avro`,
    `scio-google-cloud-platform`,
    `scio-parquet`,
    `scio-smb`,
    `scio-test` % "compile->test",
    `scio-extra`
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(siteSettings)

// =======================================================================
// Site settings
// =======================================================================

// ScalaDoc links look like http://site/index.html#my.package.MyClass while JavaDoc links look
// like http://site/my/package/MyClass.html. Therefore we need to fix links to external JavaDoc
// generated by ScalaDoc.
def fixJavaDocLinks(bases: Seq[String], doc: String): String =
  bases.foldLeft(doc) { (d, base) =>
    val regex = s"""\"($base)#([^"]*)\"""".r
    regex.replaceAllIn(
      d,
      m => {
        val b = base.replaceAll("/index.html$", "")
        val c = m.group(2).replace(".", "/")
        s"$b/$c.html"
      }
    )
  }

lazy val soccoIndex = taskKey[File]("Generates examples/index.html")

lazy val siteSettings = Def.settings(
  publish / skip := true,
  description := "Scio - Documentation",
  autoAPIMappings := true,
  gitRemoteRepo := "git@github.com:spotify/scio.git",
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
    "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion
  ),
  // unidoc
  ScalaUnidoc / siteSubdirName := "api",
  ScalaUnidoc / scalacOptions := Seq.empty,
  ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
    `scio-core`,
    `scio-test`,
    `scio-avro`,
    `scio-google-cloud-platform`,
    `scio-cassandra3`,
    `scio-elasticsearch8`,
    `scio-extra`,
    `scio-jdbc`,
    `scio-parquet`,
    `scio-tensorflow`,
    `scio-macros`,
    `scio-smb`
  ),
  // unidoc handles class paths differently than compile and may give older
  // versions high precedence.
  ScalaUnidoc / unidoc / unidocAllClasspaths := (ScalaUnidoc / unidoc / unidocAllClasspaths).value
    .map { cp =>
      cp.filterNot(_.data.getCanonicalPath.matches(""".*guava-11\..*"""))
        .filterNot(_.data.getCanonicalPath.matches(""".*bigtable-client-core-0\..*"""))
    },
  // mdoc
  // pre-compile md using mdoc
  mdocIn := (paradox / sourceDirectory).value,
  mdocExtraArguments ++= Seq("--no-link-hygiene"),
  // paradox
  paradox / sourceManaged := mdocOut.value,
  paradoxProperties ++= Map(
    "javadoc.com.spotify.scio.base_url" -> "http://spotify.github.com/scio/api",
    "javadoc.org.apache.beam.sdk.extensions.smb.base_url" ->
      "https://spotify.github.io/scio/api/org/apache/beam/sdk/extensions/smb",
    "javadoc.org.apache.beam.base_url" -> s"https://beam.apache.org/releases/javadoc/$beamVersion",
    "scaladoc.com.spotify.scio.base_url" -> "https://spotify.github.io/scio/api",
    "github.base_url" -> "https://github.com/spotify/scio",
    "extref.example.base_url" -> "https://spotify.github.io/scio/examples/%s.scala.html"
  ),
  Compile / paradoxMaterialTheme := ParadoxMaterialTheme()
    .withFavicon("images/favicon.ico")
    .withColor("white", "indigo")
    .withLogo("images/logo.png")
    .withCopyright("Copyright (C) 2020 Spotify AB")
    .withRepository(uri("https://github.com/spotify/scio"))
    .withSocial(uri("https://github.com/spotify"), uri("https://twitter.com/spotifyeng")),
  // sbt-site
  addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, ScalaUnidoc / siteSubdirName),
  makeSite / mappings ++= Seq(
    file("scio-examples/target/site/index.html") -> "examples/index.html"
  ) ++ SoccoIndex.mappings,
  makeSite := makeSite.dependsOn(mdoc.toTask("")).value
)

lazy val soccoSettings = if (sys.env.contains("SOCCO")) {
  Seq(
    scalacOptions ++= Seq(
      "-P:socco:out:scio-examples/target/site",
      "-P:socco:package_com.spotify.scio:https://spotify.github.io/scio/api"
    ),
    autoCompilerPlugins := true,
    addCompilerPlugin(("io.regadas" %% "socco-ng" % "0.1.7").cross(CrossVersion.full)),
    // Generate scio-examples/target/site/index.html
    soccoIndex := SoccoIndex.generate(target.value / "site" / "index.html"),
    Compile / compile := {
      val _ = soccoIndex.value
      (Compile / compile).value
    }
  )
} else {
  Nil
}

// strict should only be enabled when updating/adding dependencies
// ThisBuild / conflictManager := ConflictManager.strict
// To update this list we need to check against the dependencies being evicted
ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.google.api" % "api-common" % googleApiCommonVersion,
  "com.google.api" % "gax" % gaxVersion,
  "com.google.api" % "gax-grpc" % gaxVersion,
  "com.google.api" % "gax-httpjson" % gaxHttpJsonVersion,
  "com.google.api-client" % "google-api-client" % googleClientsVersion,
  "com.google.api.grpc" % "grpc-google-common-protos" % googleCommonsProtoVersion,
  "com.google.api.grpc" % "proto-google-cloud-bigtable-admin-v2" % googleCloudBigTableVersion,
  "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % googleCloudBigTableVersion,
  "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % googleCloudDatastoreVersion,
  "com.google.api.grpc" % "proto-google-common-protos" % googleCommonsProtoVersion,
  "com.google.api.grpc" % "proto-google-iam-v1" % googleIAMVersion,
  "com.google.apis" % "google-api-services-storage" % googleApiServicesStorageVersion,
  "com.google.auth" % "google-auth-library-credentials" % googleAuthVersion,
  "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
  "com.google.auto.value" % "auto-value" % autoValueVersion,
  "com.google.auto.value" % "auto-value-annotations" % autoValueVersion,
  "com.google.cloud" % "google-cloud-core" % googleCloudCoreVersion,
  "com.google.cloud" % "google-cloud-monitoring" % googleCloudMonitoringVersion,
  "com.google.cloud.bigdataoss" % "gcsio" % bigdataossVersion,
  "com.google.cloud.bigdataoss" % "util" % bigdataossVersion,
  "com.google.errorprone" % "error_prone_annotations" % errorProneAnnotationsVersion,
  "com.google.flogger" % "flogger" % floggerVersion,
  "com.google.flogger" % "flogger-system-backend" % floggerVersion,
  "com.google.flogger" % "google-extensions" % floggerVersion,
  "com.google.guava" % "guava" % guavaVersion,
  "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
  "com.google.http-client" % "google-http-client-gson" % googleHttpClientsVersion,
  "com.google.http-client" % "google-http-client-jackson2" % googleHttpClientsVersion,
  "com.google.http-client" % "google-http-client-protobuf" % googleHttpClientsVersion,
  "com.google.protobuf" % "protobuf-java" % protobufVersion,
  "com.google.protobuf" % "protobuf-java-util" % protobufVersion,
  "commons-codec" % "commons-codec" % commonsCodecVersion,
  "io.dropwizard.metrics" % "metrics-core" % metricsVersion,
  "io.grpc" % "grpc-all" % grpcVersion,
  "io.grpc" % "grpc-alts" % grpcVersion,
  "io.grpc" % "grpc-api" % grpcVersion,
  "io.grpc" % "grpc-auth" % grpcVersion,
  "io.grpc" % "grpc-context" % grpcVersion,
  "io.grpc" % "grpc-core" % grpcVersion,
  "io.grpc" % "grpc-grpclb" % grpcVersion,
  "io.grpc" % "grpc-netty" % grpcVersion,
  "io.grpc" % "grpc-netty-shaded" % grpcVersion,
  "io.grpc" % "grpc-okhttp" % grpcVersion,
  "io.grpc" % "grpc-protobuf" % grpcVersion,
  "io.grpc" % "grpc-stub" % grpcVersion,
  "io.grpc" % "grpc-xds" % grpcVersion,
  "io.netty" % "netty-all" % nettyVersion,
  "io.netty" % "netty-buffer" % nettyVersion,
  "io.netty" % "netty-codec" % nettyVersion,
  "io.netty" % "netty-codec-http" % nettyVersion,
  "io.netty" % "netty-codec-http2" % nettyVersion,
  "io.netty" % "netty-common" % nettyVersion,
  "io.netty" % "netty-handler" % nettyVersion,
  "io.netty" % "netty-resolver" % nettyVersion,
  "io.netty" % "netty-tcnative-boringssl-static" % nettyTcNativeVersion,
  "io.netty" % "netty-transport" % nettyVersion,
  "io.opencensus" % "opencensus-api" % opencensusVersion,
  "io.opencensus" % "opencensus-contrib-grpc-metrics" % opencensusVersion,
  "io.opencensus" % "opencensus-contrib-grpc-util" % opencensusVersion,
  "io.opencensus" % "opencensus-contrib-http-util" % opencensusVersion,
  "io.perfmark" % "perfmark-api" % perfmarkVersion,
  "org.checkerframework" % "checker-qual" % checkerFrameworkVersion,
  "org.codehaus.mojo" % "animal-sniffer-annotations" % animalSnifferAnnotationsVersion
)
