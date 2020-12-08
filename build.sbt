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
import de.heikoseeberger.sbtheader.CommentCreator

ThisBuild / turbo := true

val algebirdVersion = "0.13.7"
val algebraVersion = "2.0.1"
val annoy4sVersion = "0.9.0"
val annoyVersion = "0.2.6"
val asmVersion = "4.13"
val autoServiceVersion = "1.0-rc7"
val autoValueVersion = "1.7.4"
val avroVersion = "1.8.2"
val beamVendorVersion = "0.1"
val beamVersion = "2.25.0"
val bigdataossVersion = "2.1.5"
val bigQueryStorageVersion = "0.133.0-beta"
val bigtableClientVersion = "1.16.0"
val breezeVersion = "1.1"
val caffeineVersion = "2.8.8"
val caseappVersion = "2.0.4"
val catsVersion = "2.1.1"
val chillVersion = "0.9.5"
val circeVersion = "0.13.0"
val commonsCompressVersion = "1.20"
val commonsIoVersion = "2.8.0"
val commonsLang3Version = "3.11"
val commonsMath3Version = "3.6.1"
val commonsTextVersion = "1.9"
val datastoreV1ProtoClientVersion = "1.6.3"
val elasticsearch6Version = "6.8.13"
val elasticsearch7Version = "7.10.0"
val featranVersion = "0.7.0"
val flinkVersion = "1.10.1"
val gaxVersion = "1.57.1"
val gcsVersion = "1.8.0"
val generatedGrpcBetaVersion = "1.9.1"
val generatedDatastoreProtoVersion = "0.85.0"
val generatedGrpcGaVersion = "1.85.1"
val googleApiServicesBigQuery = "v2-rev20200719-1.30.10"
val googleApiServicesDataflow = "v1b3-rev20200713-1.30.10"
val googleAuthVersion = "0.19.0"
val googleClientsVersion = "1.30.10"
val googleCloudCoreVersion = "1.93.7"
val googleCloudSpannerVersion = "1.59.0"
val googleHttpClientsVersion = "1.34.0"
val googleOauthClientVersion = "1.31.0"
val grpcVersion = "1.32.1"
val guavaVersion = "28.2-jre"
val hadoopVersion = "2.8.5"
val hamcrestVersion = "2.2"
val httpCoreVersion = "4.4.13"
val jacksonVersion = "2.10.5"
val javaLshVersion = "0.12"
val jlineVersion = "2.14.6"
val jnaVersion = "5.6.0"
val jodaTimeVersion = "2.10.8"
val junitInterfaceVersion = "0.11"
val junitVersion = "4.13.1"
val kantanCodecsVersion = "0.5.1"
val kantanCsvVersion = "0.6.1"
val kryoVersion =
  "4.0.2" // explicitly depend on 4.0.1+ due to https://github.com/EsotericSoftware/kryo/pull/516
val magnoliaVersion = "0.17.0"
val magnolifyVersion = "0.3.0"
val nettyVersion = "4.1.51.Final"
val nettyTcNativeVersion = "2.0.33.Final"
val opencensusVersion = "0.24.0"
val parquetAvroVersion = "0.3.4"
val parquetExtraVersion = "0.3.4"
val parquetVersion = "1.11.1"
val protobufGenericVersion = "0.2.9"
val protobufVersion = "3.14.0"
val scalacheckVersion = "1.15.1"
val scalaMacrosVersion = "2.1.1"
val scalatestplusVersion = "3.1.0.0-RC2"
val scalatestVersion = "3.2.3"
val shapelessVersion = "2.3.3"
val slf4jVersion = "1.7.30"
val sparkeyVersion = "3.2.0"
val sparkVersion = "2.4.6"
val tensorFlowVersion = "1.15.0"
val zoltarVersion = "0.5.6"
val scalaCollectionCompatVersion = "2.3.1"

ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value)
val excludeLint = SettingKey[Set[Def.KeyedInitialize[_]]]("excludeLintKeys")
Global / excludeLint := (Global / excludeLint).?.value.getOrElse(Set.empty)
Global / excludeLint += sonatypeProfileName
Global / excludeLint += site / Paradox / sourceManaged

def previousVersion(currentVersion: String): Option[String] = {
  val Version = """(\d+)\.(\d+)\.(\d+).*""".r
  val Version(x, y, z) = currentVersion
  if (z == "0") None else Some(s"$x.$y.${z.toInt - 1}")
}

lazy val mimaSettings = Def.settings(
  mimaPreviousArtifacts :=
    previousVersion(version.value)
      .filter(_ => publishArtifact.value)
      .map(organization.value % s"${normalizedName.value}_${scalaBinaryVersion.value}" % _)
      .toSet
)

lazy val formatSettings = Def.settings(scalafmtOnCompile := false, javafmtOnCompile := false)

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
    headerLicense := Some(HeaderLicense.ALv2("2020", "Spotify AB")),
    headerMappings := headerMappings.value + (HeaderFileType.scala -> keepExistingHeader, HeaderFileType.java -> keepExistingHeader),
    scalaVersion := "2.13.3",
    crossScalaVersions := Seq("2.12.12", scalaVersion.value),
    scalacOptions ++= Scalac.commonsOptions.value,
    Compile / doc / scalacOptions --= Seq("-release", "8"),
    Compile / doc / scalacOptions ++= Scalac.compileDocOptions.value,
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
    javacOptions in (Compile, doc) := Seq("-source", "1.8"),
    // protobuf-lite is an older subset of protobuf-java and causes issues
    excludeDependencies += "com.google.protobuf" % "protobuf-lite",
    resolvers += Resolver.sonatypeRepo("public"),
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
      "com\\.spotify\\.scio\\.util\\.MultiJoin"
    ) ++ (2 to 10).map(x => s"com\\.spotify\\.scio\\.sql\\.Query${x}")).mkString(";"),
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
      )
    ),
    mimaSettings,
    formatSettings
  )

lazy val publishSettings = Def.settings(
  // Release settings
  sonatypeProfileName := "com.spotify"
)

lazy val itSettings = Def.settings(
  Defaults.itSettings,
  IntegrationTest / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  // exclude all sources if we don't have GCP credentials
  (excludeFilter in unmanagedSources) in IntegrationTest := {
    if (BuildCredentials.exists) {
      HiddenFileFilter
    } else {
      HiddenFileFilter || "*.scala"
    }
  },
  inConfig(IntegrationTest)(run / fork := true),
  inConfig(IntegrationTest)(BloopDefaults.configSettings),
  inConfig(IntegrationTest)(scalafmtConfigSettings),
  inConfig(IntegrationTest)(scalafixConfigSettings(IntegrationTest))
)

lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly ~= { old =>
    {
      case s if s.endsWith(".properties")            => MergeStrategy.filterDistinctLines
      case s if s.endsWith("public-suffix-list.txt") => MergeStrategy.filterDistinctLines
      case s if s.endsWith("pom.xml")                => MergeStrategy.last
      case s if s.endsWith(".class")                 => MergeStrategy.last
      case s if s.endsWith(".proto")                 => MergeStrategy.last
      case s if s.endsWith("libjansi.jnilib")        => MergeStrategy.last
      case s if s.endsWith("jansi.dll")              => MergeStrategy.rename
      case s if s.endsWith("libjansi.so")            => MergeStrategy.rename
      case s if s.endsWith("libsnappyjava.jnilib")   => MergeStrategy.last
      case s if s.endsWith("libsnappyjava.so")       => MergeStrategy.last
      case s if s.endsWith("snappyjava_snappy.dll")  => MergeStrategy.last
      case s if s.endsWith("reflection-config.json") => MergeStrategy.rename
      case s if s.endsWith(".dtd")                   => MergeStrategy.rename
      case s if s.endsWith(".xsd")                   => MergeStrategy.rename
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
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
lazy val flinkRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-flink-1.10" % beamVersion excludeAll (
    ExclusionRule("com.twitter", "chill_2.11"),
    ExclusionRule("org.apache.flink", "flink-clients_2.11"),
    ExclusionRule("org.apache.flink", "flink-runtime_2.11"),
    ExclusionRule("org.apache.flink", "flink-streaming-java_2.11")
  ),
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-runtime" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion
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

lazy val protobufSettings = Def.settings(
  version in ProtobufConfig := protobufVersion,
  protobufRunProtoc in ProtobufConfig := (args =>
    com.github.os72.protocjar.Protoc.runProtoc("-v3.11.4" +: args.toArray)
  )
)

def splitTests(tests: Seq[TestDefinition], filter: Seq[String], forkOptions: ForkOptions) = {
  val (filtered, default) = tests.partition(test => filter.contains(test.name))
  val policy = Tests.SubProcess(forkOptions)
  new Tests.Group(name = "<default>", tests = default, runPolicy = policy) +: filtered.map { test =>
    new Tests.Group(name = test.name, tests = Seq(test), runPolicy = policy)
  }
}

lazy val root: Project = Project("scio", file("."))
  .settings(commonSettings)
  .settings(publish / skip := true, assembly / aggregate := false)
  .aggregate(
    `scio-core`,
    `scio-test`,
    `scio-avro`,
    `scio-google-cloud-platform`,
    `scio-cassandra3`,
    `scio-elasticsearch6`,
    `scio-elasticsearch7`,
    `scio-extra`,
    `scio-jdbc`,
    `scio-parquet`,
    `scio-tensorflow`,
    `scio-schemas`,
    `scio-sql`,
    `scio-examples`,
    `scio-repl`,
    `scio-jmh`,
    `scio-macros`,
    `scio-smb`,
    `scio-redis`
  )

lazy val `scio-core`: Project = project
  .in(file("scio-core"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(
    description := "Scio - A Scala API for Apache Beam and Google Cloud Dataflow",
    resources in Compile ++= Seq(
      (baseDirectory in ThisBuild).value / "build.sbt",
      (baseDirectory in ThisBuild).value / "version.sbt"
    ),
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.github.alexarchambault" %% "case-app" % caseappVersion,
      "com.github.alexarchambault" %% "case-app-annotations" % caseappVersion,
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % "provided",
      "com.google.api-client" % "google-api-client" % googleClientsVersion,
      "com.google.apis" % "google-api-services-dataflow" % googleApiServicesDataflow,
      "com.google.auto.service" % "auto-service" % autoServiceVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
      "com.google.http-client" % "google-http-client-jackson2" % googleHttpClientsVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" % "chill-protobuf" % chillVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" %% "chill-algebird" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "io.grpc" % "grpc-auth" % grpcVersion,
      "io.grpc" % "grpc-core" % grpcVersion,
      "io.grpc" % "grpc-netty" % grpcVersion,
      "io.grpc" % "grpc-api" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion,
      "io.netty" % "netty-handler" % nettyVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-runners-core-construction-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Provided,
      "org.apache.beam" % "beam-runners-spark" % beamVersion % Provided exclude (
        "com.fasterxml.jackson.module", "jackson-module-scala_2.11"
      ),
      "org.apache.beam" % "beam-runners-flink-1.10" % beamVersion % Provided excludeAll (
        ExclusionRule("com.twitter", "chill_2.11"),
        ExclusionRule("org.apache.flink", "flink-clients_2.11"),
        ExclusionRule("org.apache.flink", "flink-runtime_2.11"),
        ExclusionRule("org.apache.flink", "flink-streaming-java_2.11")
      ),
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-protobuf" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.apache.commons" % "commons-compress" % commonsCompressVersion,
      "org.apache.commons" % "commons-math3" % commonsMath3Version,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.typelevel" %% "algebra" % algebraVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "com.propensive" %% "magnolia" % magnoliaVersion
    ),
    buildInfoKeys := Seq[BuildInfoKey](scalaVersion, version, "beamVersion" -> beamVersion),
    buildInfoPackage := "com.spotify.scio"
  )
  .dependsOn(
    `scio-schemas` % "test->test",
    `scio-macros`
  )
  .configs(
    IntegrationTest
  )
  .enablePlugins(BuildInfoPlugin)

lazy val `scio-sql`: Project = project
  .in(file("scio-sql"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio - SQL extension",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
      "org.apache.commons" % "commons-lang3" % commonsLang3Version,
      "org.apache.beam" % "beam-vendor-calcite-1_20_0" % beamVendorVersion
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
  .settings(publishSettings)
  .settings(itSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio helpers for ScalaTest",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % "test,it",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test" classifier "tests",
      "org.scalatest" %% "scalatest" % scalatestVersion,
      "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplusVersion % "test,it",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion % "it",
      "com.spotify" %% "magnolify-guava" % magnolifyVersion,
      // DataFlow testing requires junit and hamcrest
      "org.hamcrest" % "hamcrest-core" % hamcrestVersion,
      "org.hamcrest" % "hamcrest-library" % hamcrestVersion,
      // Our BloomFilters are Algebird Monoids and hence uses tests from Algebird Test
      "com.twitter" %% "algebird-test" % algebirdVersion % "test",
      "com.spotify" % "annoy" % annoyVersion % "test",
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion % "test",
      "com.novocode" % "junit-interface" % junitInterfaceVersion,
      "junit" % "junit" % junitVersion % "test",
      "com.lihaoyi" %% "pprint" % "0.6.0",
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % generatedGrpcBetaVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.twitter" %% "chill" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.hamcrest" % "hamcrest" % hamcrestVersion,
      "org.scalactic" %% "scalactic" % "3.2.3",
      "com.propensive" %% "magnolia" % magnoliaVersion
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
    `scio-schemas` % "test;it",
    `scio-avro` % "compile->test;it->it",
    `scio-sql` % "compile->test;it->it"
  )

lazy val `scio-macros`: Project = project
  .in(file("scio-macros"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio macros",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "com.propensive" %% "magnolia" % magnoliaVersion
    )
  )

lazy val `scio-avro`: Project = project
  .in(file("scio-avro"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for working with Avro",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
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
    )
  )
  .dependsOn(
    `scio-core` % "compile;it->it"
  )
  .configs(IntegrationTest)

lazy val `scio-google-cloud-platform`: Project = project
  .in(file("scio-google-cloud-platform"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(macroSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Scio add-on for Google Cloud Platform",
    libraryDependencies ++= Seq(
      "com.google.cloud" % "google-cloud-spanner" % googleCloudSpannerVersion excludeAll (
        ExclusionRule(organization = "io.grpc")
      ),
      "com.google.cloud.bigtable" % "bigtable-client-core" % bigtableClientVersion excludeAll (
        ExclusionRule(organization = "io.grpc")
      ),
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.google.api-client" % "google-api-client" % googleClientsVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigquerystorage-v1beta1" % "0.98.0",
      "com.google.api.grpc" % "proto-google-cloud-bigtable-admin-v2" % generatedGrpcBetaVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % generatedGrpcBetaVersion,
      "com.google.api" % "gax-grpc" % gaxVersion,
      "com.google.api" % "gax" % gaxVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "com.google.auth" % "google-auth-library-credentials" % googleAuthVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
      "com.google.cloud" % "google-cloud-bigquerystorage" % bigQueryStorageVersion,
      "com.google.cloud" % "google-cloud-core" % googleCloudCoreVersion,
      "com.google.cloud" % "google-cloud-storage" % gcsVersion % "test,it",
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client-jackson" % "1.29.2",
      "com.google.http-client" % "google-http-client-jackson2" % googleHttpClientsVersion,
      "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.spotify" %% "magnolify-cats" % magnolifyVersion % "test",
      "com.spotify" %% "magnolify-scalacheck" % magnolifyVersion % "test",
      "com.twitter" %% "chill" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "junit" % "junit" % junitVersion % "test",
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.hamcrest" % "hamcrest-core" % hamcrestVersion % "test,it",
      "org.hamcrest" % "hamcrest-library" % hamcrestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test,it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplusVersion % "test,it",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it"
    )
  )
  .dependsOn(
    `scio-core` % "compile;it->it",
    `scio-test` % "test;it"
  )
  .configs(IntegrationTest)

lazy val `scio-cassandra3`: Project = project
  .in(file("scio-cassandra/cassandra3"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(
    description := "Scio add-on for Apache Cassandra 3.x",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.10.2",
      ("org.apache.cassandra" % "cassandra-all" % "3.11.9")
        .exclude("ch.qos.logback", "logback-classic")
        .exclude("org.slf4j", "log4j-over-slf4j"),
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % Test,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.twitter" % "chill-java" % chillVersion
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test;it"
  )
  .configs(IntegrationTest)

lazy val `scio-elasticsearch6`: Project = project
  .in(file("scio-elasticsearch/es6"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
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
  .settings(publishSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.elasticsearch" % "elasticsearch-x-content" % elasticsearch7Version,
      "org.elasticsearch.client" % "elasticsearch-rest-client" % elasticsearch7Version,
      "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % elasticsearch7Version,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch7Version
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
  )

lazy val `scio-extra`: Project = project
  .in(file("scio-extra"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio extra utilities",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sketching" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-zetasketch" % beamVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "org.apache.avro" % "avro" % avroVersion,
      "com.spotify" % "annoy" % annoyVersion,
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "info.debatty" % "java-lsh" % javaLshVersion,
      "net.pishen" %% "annoy4s" % annoy4sVersion,
      "org.scalanlp" %% "breeze" % breezeVersion,
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % "test",
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "net.java.dev.jna" % "jna" % jnaVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.typelevel" %% "algebra" % algebraVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion
    ),
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
    `scio-google-cloud-platform`,
    `scio-macros`
  )
  .configs(IntegrationTest)

lazy val `scio-jdbc`: Project = project
  .in(file("scio-jdbc"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    description := "Scio add-on for JDBC",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
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
  .settings(publishSettings)
  .settings(
    // change annotation processor output directory so IntelliJ can pick them up
    ensureSourceManaged := IO.createDirectory(sourceManaged.value / "main"),
    (compile in Compile) := Def.task {
      val _ = ensureSourceManaged.value
      (compile in Compile).value
    }.value,
    javacOptions ++= Seq("-s", (sourceManaged.value / "main").toString),
    description := "Scio add-on for Parquet",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "me.lyh" %% "parquet-avro" % parquetExtraVersion,
      "me.lyh" % "parquet-tensorflow" % parquetExtraVersion,
      "com.google.cloud.bigdataoss" % "gcs-connector" % s"hadoop2-$bigdataossVersion",
      "org.apache.beam" % "beam-sdks-java-io-hadoop-format" % beamVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "com.twitter" %% "chill" % chillVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-hadoop-common" % beamVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.parquet" % "parquet-common" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-avro`,
    `scio-schemas` % "test",
    `scio-test` % "test->test"
  )

lazy val `scio-tensorflow`: Project = project
  .in(file("scio-tensorflow"))
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
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
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
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion % Test,
      "com.spotify" % "zoltar-core" % zoltarVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.tensorflow" % "libtensorflow" % tensorFlowVersion
    )
  )
  .dependsOn(
    `scio-avro`,
    `scio-core`,
    `scio-test` % "test->test"
  )
  .enablePlugins(ProtobufPlugin)

lazy val `scio-schemas`: Project = project
  .in(file("scio-schemas"))
  .settings(commonSettings)
  .settings(protobufSettings)
  .settings(
    description := "Avro/Proto schemas for testing",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.avro" % "avro" % avroVersion
    ),
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
  .settings(soccoSettings)
  .settings(beamRunnerSettings)
  .settings(macroSettings)
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "com.google.cloud.datastore" % "datastore-v1-proto-client" % datastoreV1ProtoClientVersion,
      "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
      "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % generatedDatastoreProtoVersion,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % generatedGrpcBetaVersion,
      "com.google.cloud.sql" % "mysql-socket-factory" % "1.2.0",
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "org.tensorflow" % "proto" % tensorFlowVersion,
      "com.spotify" %% "magnolify-avro" % magnolifyVersion,
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion,
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion,
      "com.spotify" %% "magnolify-bigtable" % magnolifyVersion,
      "mysql" % "mysql-connector-java" % "8.0.22",
      "joda-time" % "joda-time" % jodaTimeVersion,
      "com.github.alexarchambault" %% "case-app" % caseappVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.github.alexarchambault" %% "case-app-annotations" % caseappVersion,
      "com.github.alexarchambault" %% "case-app-util" % caseappVersion,
      "com.google.api-client" % "google-api-client" % googleClientsVersion,
      "com.google.apis" % "google-api-services-pubsub" % "v1-rev20191111-1.28.0",
      "com.google.auth" % "google-auth-library-credentials" % googleAuthVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
      "com.google.cloud.bigdataoss" % "util" % bigdataossVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.oauth-client" % "google-oauth-client" % googleOauthClientVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.spotify" %% "magnolify-shared" % magnolifyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.elasticsearch" % "elasticsearch" % elasticsearch7Version,
      "com.propensive" %% "magnolia" % magnoliaVersion
    ),
    // exclude problematic sources if we don't have GCP credentials
    excludeFilter in unmanagedSources := {
      if (BuildCredentials.exists) {
        HiddenFileFilter
      } else {
        HiddenFileFilter || "TypedBigQueryTornadoes*.scala" || "TypedStorageBigQueryTornadoes*.scala"
      }
    },
    sources in doc in Compile := List(),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    Test / testGrouping := splitTests(
      (Test / definedTests).value,
      List("com.spotify.scio.examples.WordCountTest"),
      ForkOptions().withRunJVMOptions((Test / javaOptions).value.toVector)
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-google-cloud-platform`,
    `scio-schemas`,
    `scio-jdbc`,
    `scio-extra`,
    `scio-elasticsearch7`,
    `scio-tensorflow`,
    `scio-sql`,
    `scio-test` % "compile->test",
    `scio-smb`,
    `scio-redis`
  )

lazy val `scio-repl`: Project = project
  .in(file("scio-repl"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(macroSettings)
  .settings(
    scalacOptions --= Seq("-release", "8"),
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion excludeAll (
        ExclusionRule("com.google.cloud.bigdataoss", "gcsio")
      ),
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion excludeAll (
        ExclusionRule("com.google.cloud.bigdataoss", "gcsio")
      ),
      "org.apache.avro" % "avro" % avroVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.commons" % "commons-text" % commonsTextVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion
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
  .dependsOn(
    `scio-core`,
    `scio-google-cloud-platform`,
    `scio-extra`
  )

lazy val `scio-jmh`: Project = project
  .in(file("scio-jmh"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio JMH Microbenchmarks",
    sourceDirectory in Jmh := (sourceDirectory in Test).value,
    classDirectory in Jmh := (classDirectory in Test).value,
    dependencyClasspath in Jmh := (dependencyClasspath in Test).value,
    libraryDependencies ++= directRunnerDependencies ++ Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "junit" % "junit" % junitVersion % "test",
      "org.hamcrest" % "hamcrest-core" % hamcrestVersion % "test",
      "org.hamcrest" % "hamcrest-library" % hamcrestVersion % "test",
      "org.slf4j" % "slf4j-nop" % slf4jVersion
    ),
    publish / skip := true
  )
  .dependsOn(
    `scio-core`,
    `scio-avro`
  )
  .enablePlugins(JmhPlugin)

lazy val `scio-smb`: Project = project
  .in(file("scio-smb"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Sort Merge Bucket source/sink implementations for Apache Beam",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "it,test" classifier "tests",
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      // #3260 work around for sorter memory limit until we patch upstream
      // "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-protobuf" % beamVersion,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQuery,
      "org.tensorflow" % "proto" % tensorFlowVersion,
      "com.google.auto.value" % "auto-value-annotations" % autoValueVersion,
      "com.google.auto.value" % "auto-value" % autoValueVersion,
      "javax.annotation" % "javax.annotation-api" % "1.3.2",
      "org.hamcrest" % "hamcrest-core" % hamcrestVersion % Test,
      "org.hamcrest" % "hamcrest-library" % hamcrestVersion % Test,
      "com.novocode" % "junit-interface" % junitInterfaceVersion % Test,
      "junit" % "junit" % junitVersion % Test,
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-vendor-guava-26_0-jre" % beamVendorVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % "provided"
    ),
    javacOptions ++= {
      (Compile / sourceManaged).value.mkdirs()
      Seq("-s", (Compile / sourceManaged).value.getAbsolutePath)
    },
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  .configs(
    IntegrationTest
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test;it",
    `scio-avro` % IntegrationTest
  )

lazy val `scio-redis`: Project = project
  .in(file("scio-redis"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(itSettings)
  .settings(
    description := "Scio integration with Redis",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.apache.beam" % "beam-sdks-java-io-redis" % beamVersion
    )
  )
  .dependsOn(
    `scio-core`,
    `scio-test` % "test"
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
    `scio-google-cloud-platform`,
    `scio-parquet`,
    `scio-schemas`,
    `scio-smb`,
    `scio-test`,
    `scio-extra`
  )

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
    `scio-elasticsearch6`,
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
    addCompilerPlugin(("io.regadas" %% "socco-ng" % "0.1.2").cross(CrossVersion.full)),
    // Generate scio-examples/target/site/index.html
    soccoIndex := SoccoIndex.generate(target.value / "site" / "index.html"),
    compile in Compile := {
      val _ = soccoIndex.value
      (compile in Compile).value
    }
  )
} else {
  Nil
}

//strict should only be enabled when updating/adding depedencies
// ThisBuild / conflictManager := ConflictManager.strict
//To update this list we need to check against the dependencies being evicted
ThisBuild / dependencyOverrides ++= Seq(
  "org.threeten" % "threetenbp" % "1.4.1",
  "org.conscrypt" % "conscrypt-openjdk-uber" % "2.2.1",
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.google.api-client" % "google-api-client" % googleClientsVersion,
  "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % generatedDatastoreProtoVersion,
  "com.google.api.grpc" % "proto-google-common-protos" % "1.17.0",
  "com.google.api" % "gax-grpc" % gaxVersion,
  "com.google.api" % "gax" % gaxVersion,
  "com.google.apis" % "google-api-services-storage" % "v1-rev20200611-1.30.10",
  "com.google.auth" % "google-auth-library-credentials" % googleAuthVersion,
  "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
  "com.google.auto.value" % "auto-value-annotations" % autoValueVersion,
  "com.google.cloud.bigdataoss" % "gcsio" % bigdataossVersion,
  "com.google.cloud.bigdataoss" % "util" % bigdataossVersion,
  "com.google.cloud" % "google-cloud-core-grpc" % "1.93.7",
  "com.google.cloud" % "google-cloud-core-http" % "1.93.7",
  "com.google.cloud" % "google-cloud-core" % "1.93.7",
  "com.google.cloud" % "google-cloud-storage" % gcsVersion,
  "com.google.code.findbugs" % "jsr305" % "3.0.2",
  "com.google.code.gson" % "gson" % "2.8.6",
  "com.google.errorprone" % "error_prone_annotations" % "2.3.4",
  "com.google.guava" % "guava" % guavaVersion,
  "com.google.http-client" % "google-http-client-jackson2" % googleHttpClientsVersion,
  "com.google.http-client" % "google-http-client" % googleHttpClientsVersion,
  "com.google.j2objc" % "j2objc-annotations" % "1.3",
  "com.google.oauth-client" % "google-oauth-client" % googleOauthClientVersion,
  "com.google.protobuf" % "protobuf-java-util" % protobufVersion,
  "com.google.protobuf" % "protobuf-java" % protobufVersion,
  "com.propensive" %% "magnolia" % magnoliaVersion,
  "com.squareup.okio" % "okio" % "1.13.0",
  "com.thoughtworks.paranamer" % "paranamer" % "2.8",
  "commons-cli" % "commons-cli" % "1.2",
  "commons-codec" % "commons-codec" % "1.14",
  "commons-collections" % "commons-collections" % "3.2.2",
  "commons-io" % "commons-io" % commonsIoVersion,
  "commons-lang" % "commons-lang" % "2.6",
  "commons-logging" % "commons-logging" % "1.2",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.dropwizard.metrics" % "metrics-core" % "3.2.2",
  "io.grpc" % "grpc-auth" % grpcVersion,
  "io.grpc" % "grpc-context" % grpcVersion,
  "io.grpc" % "grpc-core" % grpcVersion,
  "io.grpc" % "grpc-netty" % grpcVersion,
  "io.grpc" % "grpc-grpclb" % grpcVersion,
  "io.grpc" % "grpc-netty-shaded" % grpcVersion,
  "io.grpc" % "grpc-protobuf" % grpcVersion,
  "io.grpc" % "grpc-protobuf-lite" % grpcVersion,
  "io.grpc" % "grpc-stub" % grpcVersion,
  "io.grpc" % "grpc-api" % grpcVersion,
  "io.grpc" % "grpc-alts" % grpcVersion,
  "io.grpc" % "grpc-all" % grpcVersion,
  "io.grpc" % "grpc-okhttp" % grpcVersion,
  "io.netty" % "netty-buffer" % nettyVersion,
  "io.netty" % "netty-codec-http" % nettyVersion,
  "io.netty" % "netty-codec-http2" % nettyVersion,
  "io.netty" % "netty-codec" % nettyVersion,
  "io.netty" % "netty-common" % nettyVersion,
  "io.netty" % "netty-handler" % nettyVersion,
  "io.netty" % "netty-resolver" % nettyVersion,
  "io.netty" % "netty-transport" % nettyVersion,
  "io.netty" % "netty" % "3.7.0.Final",
  "io.netty" % "netty-tcnative-boringssl-static" % nettyTcNativeVersion,
  "io.opencensus" % "opencensus-api" % opencensusVersion,
  "io.opencensus" % "opencensus-contrib-grpc-util" % opencensusVersion,
  "io.opencensus" % "opencensus-contrib-http-util" % opencensusVersion,
  "javax.annotation" % "javax.annotation-api" % "1.3.2",
  "joda-time" % "joda-time" % jodaTimeVersion,
  "junit" % "junit" % junitVersion,
  "log4j" % "log4j" % "1.2.17",
  "net.java.dev.jna" % "jna" % jnaVersion,
  "org.apache.avro" % "avro" % avroVersion,
  "org.apache.commons" % "commons-compress" % commonsCompressVersion,
  "org.apache.commons" % "commons-lang3" % commonsLang3Version,
  "org.apache.commons" % "commons-math3" % commonsMath3Version,
  "org.apache.httpcomponents" % "httpclient" % "4.5.10",
  "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
  "org.apache.thrift" % "libthrift" % "0.9.2",
  "org.checkerframework" % "checker-qual" % "3.1.0",
  "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13",
  "org.codehaus.jackson" % "jackson-jaxrs" % "1.9.13",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
  "org.codehaus.jackson" % "jackson-xc" % "1.9.13",
  "org.codehaus.mojo" % "animal-sniffer-annotations" % "1.18",
  "org.hamcrest" % "hamcrest-core" % hamcrestVersion,
  "org.objenesis" % "objenesis" % "2.5.1",
  "org.ow2.asm" % "asm" % "5.0.4",
  "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "org.scalacheck" %% "scalacheck" % scalacheckVersion,
  "org.scalactic" %% "scalactic" % scalatestVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
  "org.tukaani" % "xz" % "1.8",
  "org.typelevel" %% "algebra" % algebraVersion,
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.xerial.snappy" % "snappy-java" % "1.1.4",
  "org.yaml" % "snakeyaml" % "1.12",
  "com.nrinaudo" %% "kantan.codecs" % kantanCodecsVersion
)
