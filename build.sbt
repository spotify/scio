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
import com.typesafe.sbt.SbtGit.GitKeys.gitRemoteRepo
import org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings
import bloop.integrations.sbt.BloopDefaults

ThisBuild / turbo := true

val beamVersion = "2.16.0"

val algebirdVersion = "0.13.6"
val annoy4sVersion = "0.9.0"
val annoyVersion = "0.2.6"
val asmVersion = "4.13"
val autoServiceVersion = "1.0-rc2"
val autoValueVersion = "1.7"
val avroVersion = "1.8.2"
val breezeVersion = "1.0"
val chillVersion = "0.9.4"
val circeVersion = "0.11.2"
val commonsIoVersion = "2.6"
val commonsMath3Version = "3.6.1"
val commonsLang3Version = "3.9"
val commonsCompressVersion = "1.19"
val commonsTextVersion = "1.8"
val elasticsearch2Version = "2.4.6"
val elasticsearch5Version = "5.6.16"
val elasticsearch6Version = "6.8.5"
val elasticsearch7Version = "7.4.2"
val featranVersion = "0.4.0"
val gcsConnectorVersion = "hadoop2-2.0.0"
val gcsVersion = "1.8.0"
val guavaVersion = "25.1-jre"
val hadoopVersion = "2.7.7"
val hamcrestVersion = "1.3"
val jacksonVersion = "2.10.1"
val javaLshVersion = "0.12"
val jlineVersion = "2.14.6"
val jodaTimeVersion = "2.10.5"
val junitInterfaceVersion = "0.11"
val junitVersion = "4.12"
val kantanCsvVersion = "0.5.1"
val kryoVersion = "4.0.2" // explicitly depend on 4.0.1+ due to https://github.com/EsotericSoftware/kryo/pull/516
val parquetAvroExtraVersion = "0.2.3"
val parquetVersion = "1.10.1"
val protobufGenericVersion = "0.2.5"
val protobufVersion = "3.11.0"
val scalacheckVersion = "1.14.2"
val scalaMacrosVersion = "2.1.1"
val scalatestVersion = "3.1.0"
val scalatestplusVersion = "3.1.0.0-RC2"
val shapelessVersion = "2.3.3"
val slf4jVersion = "1.7.29"
val sparkeyVersion = "3.0.0"
val tensorFlowVersion = "1.15.0"
val zoltarVersion = "0.5.6"
val magnoliaVersion = "0.12.2"
val magnolifyVersion = "0.1.3"
val grpcVersion = "1.17.1"
val caseappVersion = "2.0.0-M9"
val sparkVersion = "2.4.3"
val caffeineVersion = "2.8.0"
val bigtableClientVersion = "1.8.0"
val generatedGrpcGaVersion = "1.43.0"
val generatedGrpcBetaVersion = "0.44.0"
val googleClientsVersion = "1.27.0"
val googleApiServicesBigQuery = "v2-rev20181104-1.27.0"
val bigdataossVersion = "1.9.16"
val gaxVersion = "1.38.0"
val googleAuthVersion = "0.12.0"
val bigQueryStorageVersion = "0.79.0-alpha"
val httpCoreVersion = "4.4.11"
val googleCloudSpannerVersion = "1.6.0"
val datastoreV1ProtoClientVersion = "1.6.0"

lazy val mimaSettings = Seq(
  mimaPreviousArtifacts :=
    previousVersion(version.value)
      .filter(_ => publishArtifact.value)
      .map { pv =>
        organization.value % (normalizedName.value + "_" + scalaBinaryVersion.value) % pv
      }
      .toSet,
  mimaBinaryIssueFilters ++= Seq()
)

val beamSDKIODependencies = Def.settings(
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion excludeAll (
      ExclusionRule("com.google.cloud", "google-cloud-spanner"),
      ExclusionRule("com.google.cloud", "google-cloud-core"),
      ExclusionRule("com.google.api.grpc", "proto-google-cloud-spanner-admin-database-v1"),
      ExclusionRule("com.google.api.grpc", "proto-google-common-protos")
    ),
    "io.grpc" % "grpc-core" % grpcVersion,
    "io.grpc" % "grpc-context" % grpcVersion,
    "io.grpc" % "grpc-auth" % grpcVersion,
    "io.grpc" % "grpc-netty" % grpcVersion,
    "io.grpc" % "grpc-stub" % grpcVersion
  ),
  dependencyOverrides ++= Seq(
    "com.google.guava" % "guava" % guavaVersion,
    "com.google.api" % "gax" % gaxVersion,
    "com.google.api" % "gax-grpc" % gaxVersion
  )
)

val magnoliaDependencies = Def.settings(
  libraryDependencies ++= Seq(
    if (scalaBinaryVersion.value == "2.11") {
      "me.lyh" %% "magnolia" % "0.10.1-jto"
    } else {
      "com.propensive" %% "magnolia" % magnoliaVersion
    }
  )
)

def previousVersion(currentVersion: String): Option[String] = {
  val Version = """(\d+)\.(\d+)\.(\d+).*""".r
  val Version(x, y, z) = currentVersion
  if (z == "0") None
  else Some(s"$x.$y.${z.toInt - 1}")
}

lazy val scalafmtSettings = Seq(
  scalafmtOnCompile := false
)

val commonSettings = Sonatype.sonatypeSettings ++ assemblySettings ++ Seq(
  organization := "com.spotify",
  scalaVersion := "2.12.10",
  crossScalaVersions := Seq("2.11.12", scalaVersion.value),
  scalacOptions ++= Scalac.commonsOptions.value,
  scalacOptions in (Compile, doc) ++= Scalac.compileDocOptions.value,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc) := Seq("-source", "1.8"),
  // protobuf-lite is an older subset of protobuf-java and causes issues
  excludeDependencies += "com.google.protobuf" % "protobuf-lite",
  resolvers += Resolver.sonatypeRepo("public"),
  testOptions in Test += Tests.Argument("-oD"),
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
  testOptions ++= {
    if (sys.env.contains("SLOW")) {
      Nil
    } else {
      Seq(Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow"))
    }
  },
  evictionWarningOptions in update := EvictionWarningOptions.default
    .withWarnTransitiveEvictions(false),
  coverageExcludedPackages := Seq(
    "com\\.spotify\\.scio\\.examples\\..*",
    "com\\.spotify\\.scio\\.repl\\..*",
    "com\\.spotify\\.scio\\.util\\.MultiJoin"
  ).mkString(";"),
  coverageHighlighting := true,
  // Release settings
  publishTo := Some(
    if (isSnapshot.value) Opts.resolver.sonatypeSnapshots
    else Opts.resolver.sonatypeStaging
  ),
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  sonatypeProfileName := "com.spotify",
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
    )
  ),
  credentials ++= (for {
    username <- sys.env.get("SONATYPE_USERNAME")
    password <- sys.env.get("SONATYPE_PASSWORD")
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  buildInfoKeys := Seq[BuildInfoKey](scalaVersion, version, "beamVersion" -> beamVersion),
  buildInfoPackage := "com.spotify.scio"
) ++ mimaSettings ++ scalafmtSettings

lazy val itSettings = Defaults.itSettings ++ Seq(
  IntegrationTest / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  // exclude all sources if we don't have GCP credentials
  (excludeFilter in unmanagedSources) in IntegrationTest := {
    if (BuildCredentials.exists) {
      HiddenFileFilter
    } else {
      HiddenFileFilter || "*.scala"
    }
  }
) ++
  inConfig(IntegrationTest)(run / fork := true) ++
  inConfig(IntegrationTest)(BloopDefaults.configSettings) ++
  inConfig(IntegrationTest)(scalafmtConfigSettings) ++
  inConfig(IntegrationTest)(scalafixConfigSettings(IntegrationTest))

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly ~= { old =>
    {
      case s if s.endsWith(".properties")           => MergeStrategy.filterDistinctLines
      case s if s.endsWith("pom.xml")               => MergeStrategy.last
      case s if s.endsWith(".class")                => MergeStrategy.last
      case s if s.endsWith(".proto")                => MergeStrategy.last
      case s if s.endsWith("libjansi.jnilib")       => MergeStrategy.last
      case s if s.endsWith("jansi.dll")             => MergeStrategy.rename
      case s if s.endsWith("libjansi.so")           => MergeStrategy.rename
      case s if s.endsWith("libsnappyjava.jnilib")  => MergeStrategy.last
      case s if s.endsWith("libsnappyjava.so")      => MergeStrategy.last
      case s if s.endsWith("snappyjava_snappy.dll") => MergeStrategy.last
      case s if s.endsWith(".dtd")                  => MergeStrategy.rename
      case s if s.endsWith(".xsd")                  => MergeStrategy.rename
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case s => old(s)
    }
  }
)

lazy val macroSettings = Def.settings(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  libraryDependencies +=
    compilerPlugin(
      "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
    ),
  // see MacroSettings.scala
  scalacOptions += "-Xmacro-settings:cache-implicit-schemas=true"
)

lazy val directRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion
)
lazy val dataflowRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
)
lazy val sparkRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-spark" % beamVersion exclude (
    "com.fasterxml.jackson.module", "jackson-module-scala_2.11"
  ),
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
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
          case unkown           => Nil
        }.toSeq
      }
      .getOrElse(directRunnerDependencies)
  },
  libraryDependencies ++= beamRunnersEval.value
)

lazy val protobufSettings = Def.settings(
  version in ProtobufConfig := protobufVersion,
  protobufRunProtoc in ProtobufConfig := (
    args => com.github.os72.protocjar.Protoc.runProtoc("-v3.7.1" +: args.toArray)
  )
)

lazy val root: Project = Project("scio", file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    aggregate in assembly := false
  )
  .aggregate(
    `scio-core`,
    `scio-test`,
    `scio-avro`,
    `scio-bigquery`,
    `scio-bigtable`,
    `scio-cassandra2`,
    `scio-cassandra3`,
    `scio-elasticsearch2`,
    `scio-elasticsearch5`,
    `scio-elasticsearch6`,
    `scio-elasticsearch7`,
    `scio-extra`,
    `scio-jdbc`,
    `scio-parquet`,
    `scio-tensorflow`,
    `scio-schemas`,
    `scio-spanner`,
    `scio-sql`,
    `scio-examples`,
    `scio-repl`,
    `scio-jmh`,
    `scio-macros`,
    `scio-smb`
  )

lazy val `scio-core`: Project = project
  .in(file("scio-core"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(
    description := "Scio - A Scala API for Apache Beam and Google Cloud Dataflow",
    resources in Compile ++= Seq(
      (baseDirectory in ThisBuild).value / "build.sbt",
      (baseDirectory in ThisBuild).value / "version.sbt"
    ),
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Provided,
      "org.apache.beam" % "beam-runners-spark" % beamVersion % Provided exclude (
        "com.fasterxml.jackson.module", "jackson-module-scala_2.11"
      ),
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" %% "chill-algebird" % chillVersion,
      "com.twitter" % "chill-protobuf" % chillVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.commons" % "commons-math3" % commonsMath3Version,
      "org.apache.commons" % "commons-compress" % commonsCompressVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
      "org.apache.xbean" % "xbean-asm7-shaded" % asmVersion,
      "com.google.auto.service" % "auto-service" % autoServiceVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "com.google.api.grpc" % "grpc-google-cloud-pubsub-v1" % generatedGrpcGaVersion,
      "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % generatedGrpcBetaVersion,
      "com.google.http-client" % "google-http-client" % googleClientsVersion,
      "com.google.http-client" % "google-http-client-jackson2" % googleClientsVersion,
      "io.grpc" % "grpc-core" % grpcVersion,
      "io.grpc" % "grpc-auth" % grpcVersion,
      "io.grpc" % "grpc-netty" % grpcVersion,
      "com.github.alexarchambault" %% "case-app" % caseappVersion,
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % "provided",
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),
    magnoliaDependencies
  )
  .dependsOn(
    `scio-schemas` % "test->test",
    `scio-macros`
  )
  .configs(
    IntegrationTest
  )
  .enablePlugins(BuildInfoPlugin)

lazy val `scio-sql`: Project = Project(
  "scio-sql",
  file("scio-sql")
).settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio - SQL extension",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
      "org.apache.commons" % "commons-lang3" % commonsLang3Version
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-schemas` % "test->test",
    `scio-macros`
  )

lazy val `scio-test`: Project = project
  .in(file("scio-test"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio helpers for ScalaTest",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % "test,it",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test" classifier "tests",
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion,
      "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplusVersion % "test,it",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion % "it",
      // DataFlow testing requires junit and hamcrest
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion,
      // Our BloomFilters are Algebird Monoids and hence uses tests from Algebird Test
      "com.twitter" %% "algebird-test" % algebirdVersion % "test",
      "com.spotify" % "annoy" % annoyVersion % "test",
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion % "test",
      "com.novocode" % "junit-interface" % junitInterfaceVersion,
      "junit" % "junit" % junitVersion % "test"
    ),
    (Test / compileOrder) := CompileOrder.JavaThenScala
  )
  .configs(
    IntegrationTest
  )
  .dependsOn(
    `scio-core` % "test->test;compile->compile;it->it",
    `scio-schemas` % "test;it",
    `scio-avro` % "compile->test;it->it",
    `scio-sql` % "compile->test;it->it",
    `scio-bigquery` % "compile->test;it->it"
  )

lazy val `scio-macros`: Project = project
  .in(file("scio-macros"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio macros",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
      "org.apache.avro" % "avro" % avroVersion
    ),
    magnoliaDependencies
  )

lazy val `scio-avro`: Project = project
  .in(file("scio-avro"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for working with Avro",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "org.apache.avro" % "avro" % avroVersion exclude ("com.thoughtworks.paranamer", "paranamer"),
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplusVersion % "test,it",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "com.spotify" %% "magnolify-cats" % magnolifyVersion % "test",
      "com.spotify" %% "magnolify-scalacheck" % magnolifyVersion % "test"
    ),
    beamSDKIODependencies
  )
  .dependsOn(
    `scio-core` % "compile;it->it"
  )
  .configs(IntegrationTest)

lazy val `scio-bigquery`: Project = project
  .in(file("scio-bigquery"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Scio add-on for Google BigQuery",
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "com.google.cloud.bigdataoss" % "util" % bigdataossVersion,
      "com.google.api" % "gax" % gaxVersion,
      "com.google.api-client" % "google-api-client" % googleClientsVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "com.google.api.grpc" % "proto-google-cloud-bigquerystorage-v1beta1" % generatedGrpcBetaVersion,
      "com.google.http-client" % "google-http-client" % googleClientsVersion,
      "com.google.http-client" % "google-http-client-jackson2" % googleClientsVersion,
      "com.google.auth" % "google-auth-library-credentials" % googleAuthVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
      "com.google.cloud" % "google-cloud-bigquerystorage" % bigQueryStorageVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplusVersion % "test,it",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "com.spotify" %% "magnolify-cats" % magnolifyVersion % "test",
      "com.spotify" %% "magnolify-scalacheck" % magnolifyVersion % "test",
      "com.google.cloud" % "google-cloud-storage" % gcsVersion % "test,it",
      // DataFlow testing requires junit and hamcrest
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion % "test,it"
    )
  )
  .dependsOn(
    `scio-core` % "compile;it->it"
  )
  .configs(IntegrationTest)

lazy val `scio-bigtable`: Project = project
  .in(file("scio-bigtable"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for Google Cloud Bigtable",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.google.cloud.bigtable" % "bigtable-client-core" % bigtableClientVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % generatedGrpcBetaVersion,
      "com.novocode" % "junit-interface" % junitInterfaceVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion % "test",
      "junit" % "junit" % junitVersion % "test"
    ),
    beamSDKIODependencies
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test;it->it"
  )
  .configs(IntegrationTest)

lazy val `scio-cassandra2`: Project = project
  .in(file("scio-cassandra/cassandra2"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for Apache Cassandra 2.x",
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.8.0",
      ("org.apache.cassandra" % "cassandra-all" % "2.2.15")
        .exclude("ch.qos.logback", "logback-classic")
        .exclude("org.slf4j", "log4j-over-slf4j"),
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test;it"
  )
  .configs(IntegrationTest)

lazy val `scio-cassandra3`: Project = project
  .in(file("scio-cassandra/cassandra3"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for Apache Cassandra 3.x",
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.8.0",
      ("org.apache.cassandra" % "cassandra-all" % "3.11.5")
        .exclude("ch.qos.logback", "logback-classic")
        .exclude("org.slf4j", "log4j-over-slf4j"),
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % Test
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test;it"
  )
  .configs(IntegrationTest)

lazy val `scio-elasticsearch2`: Project = project
  .in(file("scio-elasticsearch/es2"))
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch2Version
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )

lazy val `scio-elasticsearch5`: Project = project
  .in(file("scio-elasticsearch/es5"))
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch5Version,
      "org.elasticsearch.client" % "transport" % elasticsearch5Version
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )

lazy val `scio-elasticsearch6`: Project = project
  .in(file("scio-elasticsearch/es6"))
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch6Version,
      "org.elasticsearch" % "elasticsearch-x-content" % elasticsearch6Version,
      "org.elasticsearch.client" % "transport" % elasticsearch6Version
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )

lazy val `scio-elasticsearch7`: Project = project
  .in(file("scio-elasticsearch/es7"))
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.elasticsearch" % "elasticsearch-x-content" % elasticsearch7Version,
      "org.elasticsearch.client" % "elasticsearch-rest-client" % elasticsearch7Version,
      "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % elasticsearch7Version
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )

lazy val `scio-extra`: Project = project
  .in(file("scio-extra"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(
    description := "Scio extra utilities",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "org.apache.avro" % "avro" % avroVersion,
      "com.spotify" % "annoy" % annoyVersion,
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "info.debatty" % "java-lsh" % javaLshVersion,
      "net.pishen" %% "annoy4s" % annoy4sVersion,
      "org.scalanlp" %% "breeze" % breezeVersion,
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % "test",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    ),
    beamSDKIODependencies,
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion),
    AvroConfig / version := avroVersion,
    AvroConfig / sourceDirectory := baseDirectory.value / "src" / "test" / "avro",
    Compile / sourceDirectories := (Compile / sourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    Compile / managedSourceDirectories := (Compile / managedSourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    Compile / doc / sources := List(), // suppress warnings
    compileOrder := CompileOrder.JavaThenScala
  )
  .dependsOn(
    `scio-core` % "compile->compile;provided->provided",
    `scio-test` % "it->it;test->test",
    `scio-avro`,
    `scio-bigquery`,
    `scio-macros`
  )
  .configs(IntegrationTest)

lazy val `scio-jdbc`: Project = project
  .in(file("scio-jdbc"))
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for JDBC",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )

val ensureSourceManaged = taskKey[Unit]("ensureSourceManaged")

lazy val `scio-parquet`: Project = project
  .in(file("scio-parquet"))
  .settings(commonSettings)
  .settings(
    // change annotation processor output directory so IntelliJ can pick them up
    ensureSourceManaged := IO.createDirectory(sourceManaged.value / "main"),
    (compile in Compile) := Def.task {
      ensureSourceManaged.value
      (compile in Compile).value
    }.value,
    javacOptions ++= Seq("-s", (sourceManaged.value / "main").toString),
    description := "Scio add-on for Parquet",
    libraryDependencies ++= Seq(
      "me.lyh" %% "parquet-avro-extra" % parquetAvroExtraVersion,
      "com.google.cloud.bigdataoss" % "gcs-connector" % gcsConnectorVersion,
      "org.apache.beam" % "beam-sdks-java-io-hadoop-format" % beamVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-avro`,
    `scio-schemas` % "test",
    `scio-test` % "test->test"
  )

lazy val `scio-spanner`: Project = project
  .in(file("scio-spanner"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Scio add-on for Google Cloud Spanner",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "com.google.cloud" % "google-cloud-spanner" % googleCloudSpannerVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "it"
    ),
    beamSDKIODependencies
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )
  .configs(IntegrationTest)

lazy val `scio-tensorflow`: Project = project
  .in(file("scio-tensorflow"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(protobufSettings)
  .settings(
    description := "Scio add-on for TensorFlow",
    Compile / sourceDirectories := (Compile / sourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    Compile / managedSourceDirectories := (Compile / managedSourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.tensorflow" % "tensorflow" % tensorFlowVersion,
      "org.tensorflow" % "proto" % tensorFlowVersion,
      "org.apache.commons" % "commons-compress" % commonsCompressVersion,
      "com.spotify" %% "featran-core" % featranVersion,
      "com.spotify" %% "featran-scio" % featranVersion,
      "com.spotify" %% "featran-tensorflow" % featranVersion,
      "com.spotify" % "zoltar-api" % zoltarVersion,
      "com.spotify" % "zoltar-tensorflow" % zoltarVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion % Test
    ),
    javaOptions += "-Dscio.ignoreVersionWarning=true"
  )
  .dependsOn(
    `scio-avro`,
    `scio-core`,
    `scio-test` % "it->it;test->test"
  )
  .configs(IntegrationTest)
  .enablePlugins(ProtobufPlugin)

lazy val `scio-schemas`: Project = project
  .in(file("scio-schemas"))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(protobufSettings)
  .settings(
    description := "Avro/Proto schemas for testing",
    version in AvroConfig := avroVersion,
    Compile / sourceDirectories := (Compile / sourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    Compile / managedSourceDirectories := (Compile / managedSourceDirectories).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    sources in doc in Compile := List(), // suppress warnings
    compileOrder := CompileOrder.JavaThenScala
  )
  .enablePlugins(ProtobufPlugin)

lazy val `scio-examples`: Project = project
  .in(file("scio-examples"))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(soccoSettings)
  .settings(beamRunnerSettings)
  .settings(macroSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "com.google.cloud.datastore" % "datastore-v1-proto-client" % datastoreV1ProtoClientVersion,
      "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % generatedGrpcBetaVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % generatedGrpcBetaVersion,
      "com.google.cloud.sql" % "mysql-socket-factory" % "1.0.15",
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "org.tensorflow" % "proto" % tensorFlowVersion,
      "com.spotify" %% "magnolify-avro" % magnolifyVersion,
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion,
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion,
      "mysql" % "mysql-connector-java" % "8.0.18",
      "joda-time" % "joda-time" % jodaTimeVersion,
      "com.github.alexarchambault" %% "case-app" % caseappVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    ),
    beamSDKIODependencies,
    // exclude problematic sources if we don't have GCP credentials
    excludeFilter in unmanagedSources := {
      if (BuildCredentials.exists) {
        HiddenFileFilter
      } else {
        HiddenFileFilter || "TypedBigQueryTornadoes*.scala" || "TypedStorageBigQueryTornadoes*.scala"
      }
    },
    sources in doc in Compile := List(),
    run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  .dependsOn(
    `scio-core`,
    `scio-bigquery`,
    `scio-bigtable`,
    `scio-schemas`,
    `scio-jdbc`,
    `scio-extra`,
    `scio-spanner`,
    `scio-tensorflow`,
    `scio-sql`,
    `scio-test` % "compile->test"
  )

lazy val `scio-repl`: Project = project
  .in(file("scio-repl"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.commons" % "commons-text" % commonsTextVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "jline" % "jline" % jlineVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
    ),
    assemblyJarName in assembly := s"scio-repl-${version.value}.jar"
  )
  .dependsOn(
    `scio-core`,
    `scio-bigquery`,
    `scio-extra`
  )

lazy val `scio-jmh`: Project = project
  .in(file("scio-jmh"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(noPublishSettings)
  .settings(
    description := "Scio JMH Microbenchmarks",
    sourceDirectory in Jmh := (sourceDirectory in Test).value,
    classDirectory in Jmh := (classDirectory in Test).value,
    dependencyClasspath in Jmh := (dependencyClasspath in Test).value,
    libraryDependencies ++= directRunnerDependencies ++ Seq(
      "junit" % "junit" % junitVersion % "test",
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion % "test",
      "org.slf4j" % "slf4j-nop" % slf4jVersion
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-avro`
  )
  .enablePlugins(JmhPlugin)

lazy val `scio-smb`: Project = project
  .in(file("scio-smb"))
  .settings(commonSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Sort Merge Bucket source/sink implementations for Apache Beam",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-core" % "it,test" classifier "tests",
      "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-protobuf" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "org.tensorflow" % "proto" % tensorFlowVersion,
      "com.google.auto.value" % "auto-value-annotations" % autoValueVersion,
      "com.google.auto.value" % "auto-value" % autoValueVersion,
      "javax.annotation" % "javax.annotation-api" % "1.3.2",
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test,
      "junit" % "junit" % "4.13-rc-2" % Test
    ),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  .configs(
    IntegrationTest
  )

lazy val site: Project = project
  .in(file("site"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(siteSettings)
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
    `scio-bigquery`,
    `scio-bigtable`,
    `scio-parquet`,
    `scio-schemas`,
    `scio-test`
  )

// =======================================================================
// Site settings
// =======================================================================

// ScalaDoc links look like http://site/index.html#my.package.MyClass while JavaDoc links look
// like http://site/my/package/MyClass.html. Therefore we need to fix links to external JavaDoc
// generated by ScalaDoc.
def fixJavaDocLinks(bases: Seq[String], doc: String): String = {
  bases.foldLeft(doc) { (d, base) =>
    val regex = s"""\"($base)#([^"]*)\"""".r
    regex.replaceAllIn(d, m => {
      val b = base.replaceAll("/index.html$", "")
      val c = m.group(2).replace(".", "/")
      s"$b/$c.html"
    })
  }
}

lazy val soccoIndex = taskKey[File]("Generates examples/index.html")

lazy val siteSettings = Def.settings(
  publish / skip := true,
  description := "Scio - Documentation",
  autoAPIMappings := true,
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
    "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion
  ),
  siteSubdirName in ScalaUnidoc := "api",
  scalacOptions in ScalaUnidoc := Seq(),
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
  gitRemoteRepo := "git@github.com:spotify/scio.git",
  mappings in makeSite ++= Seq(
    file("scio-examples/target/site/index.html") -> "examples/index.html"
  ) ++ SoccoIndex.mappings,
  // pre-compile md using mdoc
  mdocIn := baseDirectory.value / "src" / "paradox",
  mdocExtraArguments ++= Seq("--no-link-hygiene"),
  sourceDirectory in Paradox := mdocOut.value,
  makeSite := {
    // Fix JavaDoc links before makeSite
    mdoc.toTask("").value
    (doc in ScalaUnidoc).value
    val bases = javaMappings.map(m => m._3 + "/index.html")
    val t = (target in ScalaUnidoc).value
    (t ** "*.html").get.foreach { f =>
      val doc = fixJavaDocLinks(bases, IO.read(f))
      IO.write(f, doc)
    }
    makeSite.value
  },
  // Mappings from dependencies to external ScalaDoc/JavaDoc sites
  apiMappings ++= {
    def mappingFn(organization: String, name: String, apiUrl: String) = {
      (for {
        entry <- (fullClasspath in Compile).value
        module <- entry.get(moduleID.key)
        if module.organization == organization
        if module.name.startsWith(name)
      } yield entry.data).toList.map((_, url(apiUrl)))
    }
    val bootClasspath = System
      .getProperty("sun.boot.class.path")
      .split(sys.props("path.separator"))
      .map(file(_))
    val jdkMapping = Map(
      bootClasspath.find(_.getPath.endsWith("rt.jar")).get -> url(
        "http://docs.oracle.com/javase/8/docs/api/"
      )
    )
    docMappings.flatMap((mappingFn _).tupled).toMap ++ jdkMapping
  },
  unidocProjectFilter in (ScalaUnidoc, unidoc) :=
    inProjects(
      `scio-core`,
      `scio-test`,
      `scio-avro`,
      `scio-bigquery`,
      `scio-bigtable`,
      `scio-cassandra3`,
      `scio-elasticsearch6`,
      `scio-extra`,
      `scio-jdbc`,
      `scio-parquet`,
      `scio-tensorflow`,
      `scio-spanner`,
      `scio-macros`
    ),
  // unidoc handles class paths differently than compile and may give older
  // versions high precedence.
  unidocAllClasspaths in (ScalaUnidoc, unidoc) := {
    (unidocAllClasspaths in (ScalaUnidoc, unidoc)).value.map { cp =>
      cp.filterNot(_.data.getCanonicalPath.matches(""".*guava-11\..*"""))
        .filterNot(_.data.getCanonicalPath.matches(""".*bigtable-client-core-0\..*"""))
    }
  },
  paradoxProperties in Paradox ++= Map(
    "javadoc.com.spotify.scio.base_url" -> "http://spotify.github.com/scio/api",
    "javadoc.org.apache.beam.base_url" -> s"https://beam.apache.org/releases/javadoc/$beamVersion",
    "scaladoc.com.spotify.scio.base_url" -> "https://spotify.github.io/scio/api",
    "github.base_url" -> "https://github.com/spotify/scio",
    "extref.example.base_url" -> "https://spotify.github.io/scio/examples/%s.scala.html"
  ),
  sourceDirectory in Paradox in paradoxTheme := sourceDirectory.value / "paradox" / "_template",
  ParadoxMaterialThemePlugin.paradoxMaterialThemeSettings(Paradox),
  paradoxMaterialTheme in Paradox := {
    ParadoxMaterialTheme()
      .withFavicon("images/favicon.ico")
      .withColor("white", "indigo")
      .withLogo("images/logo.png")
      .withCopyright("Copyright (C) 2018 Spotify AB")
      .withRepository(uri("https://github.com/spotify/scio"))
      .withSocial(uri("https://github.com/spotify"), uri("https://twitter.com/spotifyeng"))
  }
)

lazy val soccoSettings = if (sys.env.contains("SOCCO")) {
  Seq(
    scalacOptions ++= Seq(
      "-P:socco:out:scio-examples/target/site",
      "-P:socco:package_com.spotify.scio:https://spotify.github.io/scio/api"
    ),
    autoCompilerPlugins := true,
    addCompilerPlugin("com.criteo.socco" %% "socco-plugin" % "0.1.9"),
    // Generate scio-examples/target/site/index.html
    soccoIndex := SoccoIndex.generate(target.value / "site" / "index.html"),
    compile in Compile := {
      soccoIndex.value
      (compile in Compile).value
    }
  )
} else {
  Nil
}

// =======================================================================
// API mappings
// =======================================================================

val beamMappings = Seq(
  "beam-sdks-java-core",
  "beam-runners-direct-java",
  "beam-runners-google-cloud-dataflow-java",
  "beam-sdks-java-io-google-cloud-platform"
).map { artifact =>
  ("org.apache.beam", artifact, s"https://beam.apache.org/documentation/sdks/javadoc/$beamVersion")
}
val javaMappings = beamMappings ++ Seq(
  (
    "com.google.apis",
    "google-api-services-bigquery",
    "https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest"
  ),
  (
    "com.google.apis",
    "google-api-services-dataflow",
    "https://developers.google.com/resources/api-libraries/documentation/dataflow/v1b3/java/latest"
  ),
  // FIXME: investigate why joda-time won't link
  ("joda-time", "joda-time", "http://www.joda.org/joda-time/apidocs"),
  ("org.apache.avro", "avro", "https://avro.apache.org/docs/current/api/java"),
  ("org.tensorflow", "libtensorflow", "https://www.tensorflow.org/api_docs/java/reference")
)
val scalaMappings = Seq(
  ("com.twitter", "algebird-core", "https://twitter.github.io/algebird/api"),
  ("org.scalanlp", "breeze", "http://www.scalanlp.org/api/breeze"),
  ("org.scalatest", "scalatest", "http://doc.scalatest.org/3.0.0")
)
val docMappings = javaMappings ++ scalaMappings
