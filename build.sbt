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

val beamVersion = "2.11.0"

val algebirdVersion = "0.13.5"
val annoy4sVersion = "0.8.0"
val annoyVersion = "0.2.5"
val asmVersion = "4.9"
val autoServiceVersion = "1.0-rc4"
val avroVersion = "1.8.2"
val breezeVersion = "1.0-RC2"
val chillVersion = "0.9.3"
val circeVersion = "0.11.1"
val commonsIoVersion = "2.6"
val commonsMath3Version = "3.6.1"
val elasticsearch2Version = "2.4.6"
val elasticsearch5Version = "5.6.15"
val elasticsearch6Version = "6.6.1"
val featranVersion = "0.3.0"
val gcsConnectorVersion = "hadoop2-1.9.16"
val gcsVersion = "1.8.0"
val guavaVersion = "25.1-jre"
val hadoopVersion = "2.7.7"
val hamcrestVersion = "1.3"
val jacksonScalaModuleVersion = "2.9.8"
val javaLshVersion = "0.11"
val jlineVersion = "2.14.6"
val jodaTimeVersion = "2.10.1"
val junitInterfaceVersion = "0.11"
val junitVersion = "4.12"
val kantanCsvVersion = "0.5.0"
val kryoVersion = "4.0.2" // explicitly depend on 4.0.1+ due to https://github.com/EsotericSoftware/kryo/pull/516
val parquetAvroExtraVersion = "0.2.2"
val parquetVersion = "1.10.1"
val protobufGenericVersion = "0.2.4"
val protobufVersion = "3.7.0"
val scalacheckShapelessVersion = "1.1.8"
val scalacheckVersion = "1.14.0"
val scalaMacrosVersion = "2.1.1"
val scalatestVersion = "3.0.6"
val shapelessVersion = "2.3.3"
val shapelessDatatypeVersion = "0.1.10"
val slf4jVersion = "1.7.25"
val sparkeyVersion = "3.0.0"
val tensorFlowVersion = "1.13.1"
val zoltarVersion = "0.5.1"
val magnoliaVersion = "0.10.1-jto"
val grpcVersion = "1.17.1"
val caseappVersion = "2.0.0-M6"

lazy val scalafixSettings = Def.settings(
  addCompilerPlugin(scalafixSemanticdb)
)

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
    "com.google.guava" % "guava" % guavaVersion
  )
)

def previousVersion(currentVersion: String): Option[String] = {
  val Version = """(\d+)\.(\d+)\.(\d+).*""".r
  val Version(x, y, z) = currentVersion
  if (z == "0") None
  else Some(s"$x.$y.${z.toInt - 1}")
}

lazy val scalafmtSettings = Seq(
  scalafmtOnCompile := true
)

val commonSettings = Sonatype.sonatypeSettings ++ assemblySettings ++ Seq(
  organization := "com.spotify",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.12", scalaVersion.value),
  scalacOptions ++= Scalac.commonsOptions.value,
  scalacOptions in (Compile, doc) ++= Scalac.compileDocOptions.value,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc) := Seq("-source", "1.8"),
  // protobuf-lite is an older subset of protobuf-java and causes issues
  excludeDependencies += "com.google.protobuf" % "protobuf-lite",
  resolvers += Resolver.sonatypeRepo("public"),
  scalastyleSources in Compile ++= (unmanagedSourceDirectories in Test).value,
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
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots
  else Opts.resolver.sonatypeStaging),
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  sonatypeProfileName := "com.spotify",
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/spotify/scio")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/spotify/scio"), "scm:git:git@github.com:spotify/scio.git")),
  developers := List(
    Developer(id = "sinisa_lyh",
              name = "Neville Li",
              email = "neville.lyh@gmail.com",
              url = url("https://twitter.com/sinisa_lyh")),
    Developer(id = "ravwojdyla",
              name = "Rafal Wojdyla",
              email = "ravwojdyla@gmail.com",
              url = url("https://twitter.com/ravwojdyla")),
    Developer(id = "andrewsmartin",
              name = "Andrew Martin",
              email = "andrewsmartin.mg@gmail.com",
              url = url("https://twitter.com/andrew_martin92")),
    Developer(id = "fallonfofallon",
              name = "Fallon Chen",
              email = "fallon@spotify.com",
              url = url("https://twitter.com/fallonfofallon")),
    Developer(id = "regadas",
              name = "Filipe Regadas",
              email = "filiperegadas@gmail.com",
              url = url("https://twitter.com/regadas")),
    Developer(id = "jto",
              name = "Julien Tournay",
              email = "julient@spotify.com",
              url = url("https://twitter.com/skaalf")),
    Developer(id = "clairemcginty",
              name = "Claire McGinty",
              email = "clairem@spotify.com",
              url = url("http://github.com/clairemcginty"))
  ),
  credentials ++= (for {
    username <- sys.env.get("SONATYPE_USERNAME")
    password <- sys.env.get("SONATYPE_PASSWORD")
  } yield
    Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  buildInfoKeys := Seq[BuildInfoKey](scalaVersion, version, "beamVersion" -> beamVersion),
  buildInfoPackage := "com.spotify.scio"
) ++ mimaSettings ++ scalafmtSettings ++ scalafixSettings

lazy val itSettings = Defaults.itSettings ++ Seq(
  scalastyleSources in Compile ++= (unmanagedSourceDirectories in IntegrationTest).value,
  // exclude all sources if we don't have GCP credentials
  (excludeFilter in unmanagedSources) in IntegrationTest := {
    if (BuildCredentials.exists) {
      HiddenFileFilter
    } else {
      HiddenFileFilter || "*.scala"
    }
  }
) ++
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

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full

lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)

lazy val directRunnerDependency =
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion
lazy val dataflowRunnerDependency =
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
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
          case "DirectRunner"   => Some(directRunnerDependency)
          case "DataflowRunner" => Some(dataflowRunnerDependency)
          case unkown           => None
        }.toSeq
      }
      .getOrElse(Seq(directRunnerDependency))
  },
  libraryDependencies ++= beamRunnersEval.value
)

lazy val root: Project = Project("scio", file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    aggregate in assembly := false
  )
  .aggregate(
    scioCore,
    scioTest,
    scioAvro,
    scioBigQuery,
    scioBigtable,
    scioCassandra2,
    scioCassandra3,
    scioElasticsearch2,
    scioElasticsearch5,
    scioElasticsearch6,
    scioExtra,
    scioHdfs,
    scioJdbc,
    scioParquet,
    scioTensorFlow,
    scioSchemas,
    scioSpanner,
    scioExamples,
    scioRepl,
    scioJmh,
    scioMacros
  )

lazy val scioCore: Project = Project(
  "scio-core",
  file("scio-core")
).settings(
    commonSettings ++ macroSettings ++ itSettings,
    description := "Scio - A Scala API for Apache Beam and Google Cloud Dataflow",
    resources in Compile ++= Seq((baseDirectory in ThisBuild).value / "build.sbt",
                                 (baseDirectory in ThisBuild).value / "version.sbt"),
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % "provided",
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" %% "chill-algebird" % chillVersion,
      "com.twitter" % "chill-protobuf" % chillVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.commons" % "commons-math3" % commonsMath3Version,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonScalaModuleVersion,
      "com.google.auto.service" % "auto-service" % autoServiceVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
      "org.apache.xbean" % "xbean-asm6-shaded" % asmVersion,
      "io.grpc" % "grpc-all" % grpcVersion exclude ("io.opencensus", "opencensus-api"),
      "com.github.alexarchambault" %% "case-app" % caseappVersion,
      "me.lyh" %% "magnolia" % magnoliaVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
  .dependsOn(
    scioSchemas % "test->test",
    scioMacros
  )
  .configs(
    IntegrationTest
  )
  .enablePlugins(BuildInfoPlugin)

lazy val scioTest: Project = Project(
  "scio-test",
  file("scio-test")
).settings(
    commonSettings ++ itSettings ++ macroSettings,
    description := "Scio helpers for ScalaTest",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % "test,it",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test" classifier "tests",
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalacheckShapelessVersion % "test,it",
      "me.lyh" %% "shapeless-datatype-datastore_1.3" % shapelessDatatypeVersion % "test,it",
      // DataFlow testing requires junit and hamcrest
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion,
      "com.spotify" % "annoy" % annoyVersion % "test",
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion % "test",
      "com.novocode" % "junit-interface" % junitInterfaceVersion,
      "junit" % "junit" % junitVersion % "test"
    )
  )
  .configs(
    IntegrationTest
  )
  .dependsOn(
    scioCore % "test->test;compile->compile;it->it",
    scioSchemas % "test,it",
    scioAvro % "compile->test,it->it",
    scioBigQuery % "compile->test,it->it"
  )

lazy val scioMacros: Project = Project(
  "scio-macros",
  file("scio-macros")
).settings(
  commonSettings ++ macroSettings,
  description := "Scio macros",
  libraryDependencies ++= Seq(
    "me.lyh" %% "magnolia" % magnoliaVersion,
    "com.chuusai" %% "shapeless" % shapelessVersion,
    "com.esotericsoftware" % "kryo-shaded" % kryoVersion
  )
)

lazy val scioAvro: Project = Project(
  "scio-avro",
  file("scio-avro")
).settings(
    commonSettings ++ macroSettings ++ itSettings,
    description := "Scio add-on for working with Avro",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % avroVersion exclude ("com.thoughtworks.paranamer", "paranamer"),
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalacheckShapelessVersion % "test",
      "me.lyh" %% "shapeless-datatype-core" % shapelessDatatypeVersion % "test"
    ),
    beamSDKIODependencies
  )
  .dependsOn(
    scioCore % "compile,it->it"
  )
  .configs(IntegrationTest)

lazy val scioBigQuery: Project = Project(
  "scio-bigquery",
  file("scio-bigquery")
).settings(
    commonSettings ++ macroSettings ++ itSettings ++ beamRunnerSettings,
    description := "Scio add-on for Google BigQuery",
    libraryDependencies ++= Seq(
      "commons-io" % "commons-io" % commonsIoVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "com.google.cloud" % "google-cloud-storage" % gcsVersion % "test,it",
      // DataFlow testing requires junit and hamcrest
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion % "test,it",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalacheckShapelessVersion % "test,it",
      "me.lyh" %% "shapeless-datatype-core" % shapelessDatatypeVersion % "test"
    )
  )
  .dependsOn(
    scioCore % "compile,it->it"
  )
  .configs(IntegrationTest)

lazy val scioBigtable: Project = Project(
  "scio-bigtable",
  file("scio-bigtable")
).settings(
    commonSettings ++ itSettings,
    description := "Scio add-on for Google Cloud Bigtable",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion % "test",
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % "test",
      "com.novocode" % "junit-interface" % junitInterfaceVersion,
      "junit" % "junit" % junitVersion % "test"
    ),
    beamSDKIODependencies
  )
  .dependsOn(
    scioCore,
    scioTest % "test,it->it"
  )
  .configs(IntegrationTest)

lazy val scioCassandra2: Project = Project(
  "scio-cassandra2",
  file("scio-cassandra/cassandra2")
).settings(
    commonSettings ++ itSettings,
    description := "Scio add-on for Apache Cassandra 2.x",
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.1",
      ("org.apache.cassandra" % "cassandra-all" % "2.2.14")
        .exclude("ch.qos.logback", "logback-classic")
        .exclude("org.slf4j", "log4j-over-slf4j"),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion
    )
  )
  .dependsOn(
    scioCore,
    scioTest % "test,it"
  )
  .configs(IntegrationTest)

lazy val scioCassandra3: Project = Project(
  "scio-cassandra3",
  file("scio-cassandra/cassandra3")
).settings(
    commonSettings ++ itSettings,
    description := "Scio add-on for Apache Cassandra 3.x",
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.1",
      ("org.apache.cassandra" % "cassandra-all" % "3.11.4")
        .exclude("ch.qos.logback", "logback-classic")
        .exclude("org.slf4j", "log4j-over-slf4j"),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
  .dependsOn(
    scioCore,
    scioTest % "test,it"
  )
  .configs(IntegrationTest)

lazy val scioElasticsearch2: Project = Project(
  "scio-elasticsearch2",
  file("scio-elasticsearch/es2")
).settings(
    commonSettings,
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch2Version
    )
  )
  .dependsOn(
    scioCore,
    scioTest % "test"
  )

lazy val scioElasticsearch5: Project = Project(
  "scio-elasticsearch5",
  file("scio-elasticsearch/es5")
).settings(
    commonSettings,
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.elasticsearch.client" % "transport" % elasticsearch5Version
    )
  )
  .dependsOn(
    scioCore,
    scioTest % "test"
  )

lazy val scioElasticsearch6: Project = Project(
  "scio-elasticsearch6",
  file("scio-elasticsearch/es6")
).settings(
    commonSettings,
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.elasticsearch.client" % "transport" % elasticsearch6Version
    )
  )
  .dependsOn(
    scioCore,
    scioTest % "test"
  )

lazy val scioExtra: Project = Project(
  "scio-extra",
  file("scio-extra")
).settings(
    commonSettings ++ itSettings,
    description := "Scio extra utilities",
    libraryDependencies ++= Seq(
      "com.spotify" % "annoy" % annoyVersion,
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "info.debatty" % "java-lsh" % javaLshVersion,
      "net.pishen" %% "annoy4s" % annoy4sVersion,
      "org.scalanlp" %% "breeze" % breezeVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    ),
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
    scioCore,
    scioTest % "it->it;test->test",
    scioAvro,
    scioBigQuery,
    scioMacros
  )
  .configs(IntegrationTest)

lazy val scioHdfs: Project = Project(
  "scio-hdfs",
  file("scio-hdfs")
).settings(
  commonSettings,
  description := "Scio add-on for HDFS",
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-sdks-java-io-hadoop-file-system" % beamVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion
  )
)

lazy val scioJdbc: Project = Project(
  "scio-jdbc",
  file("scio-jdbc")
).settings(
    commonSettings,
    description := "Scio add-on for JDBC",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion
    )
  )
  .dependsOn(
    scioCore,
    scioTest % "test"
  )

val ensureSourceManaged = taskKey[Unit]("ensureSourceManaged")

lazy val scioParquet: Project = Project(
  "scio-parquet",
  file("scio-parquet")
).settings(
    commonSettings,
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
      "org.apache.beam" % "beam-sdks-java-io-hadoop-input-format" % beamVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion
    )
  )
  .dependsOn(
    scioCore,
    scioAvro,
    scioSchemas % "test",
    scioTest % "test->test"
  )

lazy val scioSpanner: Project = Project(
  "scio-spanner",
  file("scio-spanner")
).settings(
    commonSettings ++ itSettings ++ beamRunnerSettings,
    description := "Scio add-on for Google Cloud Spanner",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % "it"
    )
  )
  .dependsOn(
    scioCore,
    scioTest % "test"
  )
  .configs(IntegrationTest)

lazy val scioTensorFlow: Project = Project(
  "scio-tensorflow",
  file("scio-tensorflow")
).enablePlugins(ProtobufPlugin)
  .settings(
    commonSettings ++ itSettings,
    description := "Scio add-on for TensorFlow",
    version in ProtobufConfig := protobufVersion,
    protobufRunProtoc in ProtobufConfig := (args =>
      com.github.os72.protocjar.Protoc.runProtoc("-v3.7.0" +: args.toArray)),
    sourceDirectories in Compile := (sourceDirectories in Compile).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    managedSourceDirectories in Compile := (managedSourceDirectories in Compile).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    libraryDependencies ++= Seq(
      "org.tensorflow" % "tensorflow" % tensorFlowVersion,
      "org.tensorflow" % "proto" % tensorFlowVersion,
      "me.lyh" %% "shapeless-datatype-tensorflow" % shapelessDatatypeVersion,
      "com.spotify" %% "featran-core" % featranVersion,
      "com.spotify" %% "featran-scio" % featranVersion,
      "com.spotify" %% "featran-tensorflow" % featranVersion,
      "com.spotify" % "zoltar-api" % zoltarVersion,
      "com.spotify" % "zoltar-tensorflow" % zoltarVersion
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion),
    (testLoader in Test) :=
      TensorflowTestClasspath.create((testLoader in Test).value,
                                     (dependencyClasspath in Test).value)
  )
  .dependsOn(
    scioAvro,
    scioCore,
    scioTest % "it->it;test->test"
  )
  .configs(IntegrationTest)

lazy val scioSchemas: Project = Project(
  "scio-schemas",
  file("scio-schemas")
).enablePlugins(ProtobufPlugin)
  .settings(
    commonSettings ++ noPublishSettings,
    description := "Avro/Proto schemas for testing",
    version in AvroConfig := avroVersion,
    version in ProtobufConfig := protobufVersion,
    protobufRunProtoc in ProtobufConfig := (args =>
      com.github.os72.protocjar.Protoc.runProtoc("-v3.6.0" +: args.toArray)),
    // Avro and Protobuf files are compiled to src_managed/main/compiled_{avro,protobuf}
    // Exclude their parent to avoid confusing IntelliJ
    sourceDirectories in Compile := (sourceDirectories in Compile).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    managedSourceDirectories in Compile := (managedSourceDirectories in Compile).value
      .filterNot(_.getPath.endsWith("/src_managed/main")),
    sources in doc in Compile := List(), // suppress warnings
    compileOrder := CompileOrder.JavaThenScala
  )

lazy val scioExamples: Project = Project(
  "scio-examples",
  file("scio-examples")
).settings(
    commonSettings ++ noPublishSettings ++ soccoSettings ++ beamRunnerSettings,
    libraryDependencies ++= Seq(
      "me.lyh" %% "shapeless-datatype-avro" % shapelessDatatypeVersion,
      "me.lyh" %% "shapeless-datatype-datastore_1.3" % shapelessDatatypeVersion,
      "me.lyh" %% "shapeless-datatype-tensorflow" % shapelessDatatypeVersion,
      "mysql" % "mysql-connector-java" % "8.0.15",
      "com.google.cloud.sql" % "mysql-socket-factory" % "1.0.12",
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion
    ),
    addCompilerPlugin(paradiseDependency),
    // exclude problematic sources if we don't have GCP credentials
    excludeFilter in unmanagedSources := {
      if (BuildCredentials.exists) {
        HiddenFileFilter
      } else {
        HiddenFileFilter || "TypedBigQueryTornadoes*.scala"
      }
    },
    sources in doc in Compile := List()
  )
  .dependsOn(
    scioCore,
    scioBigQuery,
    scioBigtable,
    scioSchemas,
    scioJdbc,
    scioExtra,
    scioSpanner,
    scioTensorFlow,
    scioTest % "test"
  )

lazy val scioRepl: Project = Project(
  "scio-repl",
  file("scio-repl")
).settings(
    commonSettings ++ macroSettings,
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "jline" % "jline" % jlineVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      paradiseDependency
    ),
    assemblyJarName in assembly := s"scio-repl-${version.value}.jar"
  )
  .dependsOn(
    scioCore,
    scioBigQuery,
    scioExtra
  )

lazy val scioJmh: Project = Project(
  "scio-jmh",
  file("scio-jmh")
).settings(
    commonSettings ++ noPublishSettings,
    description := "Scio JMH Microbenchmarks",
    addCompilerPlugin(paradiseDependency),
    sourceDirectory in Jmh := (sourceDirectory in Test).value,
    classDirectory in Jmh := (classDirectory in Test).value,
    dependencyClasspath in Jmh := (dependencyClasspath in Test).value,
    libraryDependencies ++= Seq(
      "junit" % "junit" % junitVersion % "test",
      "org.hamcrest" % "hamcrest-all" % hamcrestVersion % "test",
      "org.slf4j" % "slf4j-nop" % slf4jVersion,
      directRunnerDependency
    )
  )
  .dependsOn(
    scioCore,
    scioAvro
  )
  .enablePlugins(JmhPlugin)

lazy val site: Project = project
  .in(file("site"))
  .enablePlugins(ParadoxSitePlugin,
                 ParadoxMaterialThemePlugin,
                 GhpagesPlugin,
                 ScalaUnidocPlugin,
                 SiteScaladocPlugin)
  .settings(commonSettings)
  .settings(siteSettings)

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
  siteSubdirName in ScalaUnidoc := "api",
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
  gitRemoteRepo := "git@github.com:spotify/scio.git",
  mappings in makeSite ++= Seq(
    file("scio-examples/target/site/index.html") -> "examples/index.html"
  ) ++ SoccoIndex.mappings,
  makeSite := {
    // Fix JavaDoc links before makeSite
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
        "http://docs.oracle.com/javase/8/docs/api/"))
    docMappings.flatMap((mappingFn _).tupled).toMap ++ jdkMapping
  },
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject
    -- inProjects(scioCassandra2)
    -- inProjects(scioElasticsearch2, scioElasticsearch5)
    -- inProjects(scioRepl) -- inProjects(scioSchemas) -- inProjects(scioExamples)
    -- inProjects(scioJmh),
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
  ("com.google.apis",
   "google-api-services-bigquery",
   "https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest"),
  ("com.google.apis",
   "google-api-services-dataflow",
   "https://developers.google.com/resources/api-libraries/documentation/dataflow/v1b3/java/latest"),
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
