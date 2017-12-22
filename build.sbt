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

val beamVersion = "2.2.0"

val algebirdVersion = "0.13.2"
val annoy4sVersion = "0.5.0"
val annoyVersion = "0.2.5"
val asmVersion = "4.5"
val autoServiceVersion = "1.0-rc3"
val autoValueVersion = "1.4.1"
val avroVersion = "1.8.2"
val breezeVersion ="0.13.1"
val chillVersion = "0.9.2"
val circeVersion = "0.8.0"
val commonsIoVersion = "2.5"
val commonsMath3Version = "3.6.1"
val elasticsearch2Version = "2.1.0"
val elasticsearch5Version = "5.5.0"
val gcsConnectorVersion = "1.6.1-hadoop2"
val guavaVersion = "20.0"
val hadoopVersion = "2.7.3"
val hamcrestVersion = "1.3"
val jacksonScalaModuleVersion = "2.8.9"
val javaLshVersion = "0.10"
val jlineVersion = "2.14.3"
val jodaConvertVersion = "1.8.1"
val jodaTimeVersion = "2.9.9"
val junitInterfaceVersion = "0.11"
val junitVersion = "4.12"
val kantanCsvVersion = "0.3.0"
val kryoVersion = "4.0.1" // explicitly depend on 4.0.1 due to https://github.com/EsotericSoftware/kryo/pull/516
val mockitoVersion = "1.10.19"
val parquetAvroExtraVersion = "0.2.2"
val parquetVersion = "1.9.0"
val protobufGenericVersion = "0.2.2"
val protobufVersion = "3.3.1"
val scalacheckShapelessVersion = "1.1.6"
val scalacheckVersion = "1.13.5"
val scalaMacrosVersion = "2.1.0"
val scalatestVersion = "3.0.4"
val shapelessDatatypeVersion = "0.1.7"
val slf4jVersion = "1.7.25"
val sparkeyVersion = "2.1.3"
val tensorFlowVersion = "1.3.0"

val commonSettings = Sonatype.sonatypeSettings ++ assemblySettings ++ Seq(
  organization       := "com.spotify",

  scalaVersion       := "2.12.4",
  crossScalaVersions := Seq("2.11.11", "2.12.4"),
  scalacOptions                   ++= {
    Seq("-Xmax-classfile-name", "100", "-target:jvm-1.8", "-deprecation", "-feature", "-unchecked") ++
      (if (scalaBinaryVersion.value == "2.12") Seq("-Ydelambdafy:inline") else Nil)
    },
  scalacOptions in (Compile, doc) ++= {
    Seq("-skip-packages", "org.apache") ++
      (if (scalaBinaryVersion.value == "2.12") Seq("-no-java-comments") else Nil)
    },
  javacOptions                    ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc)  := Seq("-source", "1.8"),

  // protobuf-lite is an older subset of protobuf-java and causes issues
  excludeDependencies += "com.google.protobuf" % "protobuf-lite",

  resolvers += Resolver.sonatypeRepo("public"),

  scalastyleSources in Compile ++= (unmanagedSourceDirectories in Test).value,
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
  testOptions ++= {
    if (sys.env.contains("SLOW")) {
      Nil
    } else {
      Seq(Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow"))
    }
  },

  coverageExcludedPackages := Seq(
    "com\\.spotify\\.scio\\.examples\\..*",
    "com\\.spotify\\.scio\\.repl\\..*",
    "com\\.spotify\\.scio\\.util\\.MultiJoin"
  ).mkString(";"),
  coverageHighlighting := true,

  // Release settings
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging),
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  sonatypeProfileName           := "com.spotify",

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/spotify/scio")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/spotify/scio.git"),
    "scm:git:git@github.com:spotify/scio.git")),
  developers := List(
    Developer(id="sinisa_lyh", name="Neville Li", email="neville.lyh@gmail.com", url=url("https://twitter.com/sinisa_lyh")),
    Developer(id="ravwojdyla", name="Rafal Wojdyla", email="ravwojdyla@gmail.com", url=url("https://twitter.com/ravwojdyla")),
    Developer(id="andrewsmartin", name="Andrew Martin", email="andrewsmartin.mg@gmail.com", url=url("https://twitter.com/andrew_martin92")),
    Developer(id="fallonfofallon", name="Fallon Chen", email="fallon@spotify.com", url=url("https://twitter.com/fallonfofallon"))
  ),

  credentials ++= (for {
    username <- sys.env.get("SONATYPE_USERNAME")
    password <- sys.env.get("SONATYPE_PASSWORD")
  } yield
  Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    username,
    password)).toSeq,

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
    val bootClasspath = System.getProperty("sun.boot.class.path").split(sys.props("path.separator")).map(file(_))
    val jdkMapping = Map(bootClasspath.find(_.getPath.endsWith("rt.jar")).get -> url("http://docs.oracle.com/javase/8/docs/api/"))
    docMappings.flatMap((mappingFn _).tupled).toMap ++ jdkMapping
  }
)

lazy val itSettings = Defaults.itSettings ++ Seq(
  scalastyleSources in Compile ++= (unmanagedSourceDirectories in IntegrationTest).value
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly ~= { old => {
    case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
    case s if s.endsWith("pom.xml") => MergeStrategy.last
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith(".proto") => MergeStrategy.last
    case s if s.endsWith("libjansi.jnilib") => MergeStrategy.last
    case s if s.endsWith("jansi.dll") => MergeStrategy.rename
    case s if s.endsWith("libjansi.so") => MergeStrategy.rename
    case s if s.endsWith("libsnappyjava.jnilib") => MergeStrategy.last
    case s if s.endsWith("libsnappyjava.so") => MergeStrategy.last
    case s if s.endsWith("snappyjava_snappy.dll") => MergeStrategy.last
    case s if s.endsWith(".dtd") => MergeStrategy.rename
    case s if s.endsWith(".xsd") => MergeStrategy.rename
    case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
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
          case "DirectRunner" => Some(directRunnerDependency)
          case "DataflowRunner" => Some(dataflowRunnerDependency)
          case unkown => None
        }.toSeq
      }
      .getOrElse(Seq(directRunnerDependency))
  },
  libraryDependencies ++= beamRunnersEval.value
)

lazy val root: Project = Project(
  "scio",
  file(".")
).enablePlugins(GhpagesPlugin, ScalaUnidocPlugin, SiteScaladocPlugin).settings(
  commonSettings ++ siteSettings ++ noPublishSettings,
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject
    -- inProjects(scioCassandra2) -- inProjects(scioElasticsearch2)
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
  aggregate in assembly := false
).aggregate(
  scioCore,
  scioTest,
  scioAvro,
  scioBigQuery,
  scioBigtable,
  scioCassandra2,
  scioCassandra3,
  scioElasticsearch2,
  scioElasticsearch5,
  scioExtra,
  scioHdfs,
  scioJdbc,
  scioParquet,
  scioTensorFlow,
  scioSchemas,
  scioExamples,
  scioRepl,
  scioJmh
)

lazy val scioCore: Project = Project(
  "scio-core",
  file("scio-core")
).settings(
  commonSettings ++ macroSettings,
  description := "Scio - A Scala API for Apache Beam and Google Cloud Dataflow",
  resources in Compile += (baseDirectory in ThisBuild).value / "version.sbt",
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % "provided",
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter" %% "chill-algebird" % chillVersion,
    "com.twitter" % "chill-protobuf" % chillVersion,
    "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
    "commons-io" % "commons-io" % commonsIoVersion,
    "org.apache.commons" % "commons-math3" % commonsMath3Version,
    "org.tensorflow" % "proto" % tensorFlowVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonScalaModuleVersion,
    "com.google.auto.service" % "auto-service" % autoServiceVersion,
    "com.google.guava" % "guava" % guavaVersion,
    "com.google.protobuf" % "protobuf-java" % protobufVersion,
    "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
    "org.apache.xbean" % "xbean-asm5-shaded" % asmVersion
  )
).dependsOn(
  scioAvro,
  scioBigQuery
)

lazy val scioTest: Project = Project(
  "scio-test",
  file("scio-test")
).settings(
  commonSettings ++ itSettings,
  description := "Scio helpers for ScalaTest",
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % "it",
    "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % scalatestVersion,
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
    // DataFlow testing requires junit and hamcrest
    "org.hamcrest" % "hamcrest-all" % hamcrestVersion,
    "com.spotify" % "annoy" % annoyVersion % "test",
    "com.spotify.sparkey" % "sparkey" % sparkeyVersion % "test",
    "com.novocode" % "junit-interface" % junitInterfaceVersion,
    "junit" % "junit" % junitVersion % "test"
  ),
  addCompilerPlugin(paradiseDependency)
).configs(
  IntegrationTest
).dependsOn(
  scioCore,
  scioSchemas % "test"
)

lazy val scioAvro: Project = Project(
  "scio-avro",
  file("scio-avro")
).settings(
  commonSettings ++ macroSettings ++ itSettings,
  description := "Scio add-on for working with Avro",
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion,
    "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalacheckShapelessVersion % "test",
    "me.lyh" %% "shapeless-datatype-core" % shapelessDatatypeVersion % "test"
  )
).configs(IntegrationTest)

lazy val scioBigQuery: Project = Project(
  "scio-bigquery",
  file("scio-bigquery")
).settings(
  commonSettings ++ macroSettings ++ itSettings,
  description := "Scio add-on for Google BigQuery",
  libraryDependencies ++= Seq(
    "commons-io" % "commons-io" % commonsIoVersion,
    "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
    "joda-time" % "joda-time" % jodaTimeVersion,
    "org.joda" % "joda-convert" % jodaConvertVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalacheckShapelessVersion % "test",
    "me.lyh" %% "shapeless-datatype-core" % shapelessDatatypeVersion % "test"
  )
).configs(IntegrationTest)

lazy val scioBigtable: Project = Project(
  "scio-bigtable",
  file("scio-bigtable")
).settings(
  commonSettings ++ itSettings,
  description := "Scio add-on for Google Cloud Bigtable",
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )
).dependsOn(
  scioCore,
  scioTest % "it"
).configs(IntegrationTest)

lazy val scioCassandra2: Project = Project(
  "scio-cassandra2",
  file("scio-cassandra2")
).settings(
  commonSettings ++ itSettings,
  description := "Scio add-on for Apache Cassandra 2.x",
  scalaSource in Compile := (baseDirectory in ThisBuild).value / "scio-cassandra3/src/main/scala",
  libraryDependencies ++= Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.10.3",
    "org.apache.cassandra" % "cassandra-all" % "2.0.17",
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion
  )
).dependsOn(
  scioCore,
  scioTest % "it"
).configs(IntegrationTest)

lazy val scioCassandra3: Project = Project(
  "scio-cassandra3",
  file("scio-cassandra3")
).settings(
  commonSettings ++ itSettings,
  description := "Scio add-on for Apache Cassandra 3.x",
  libraryDependencies ++= Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0",
    ("org.apache.cassandra" % "cassandra-all" % "3.11.0")
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("org.slf4j", "log4j-over-slf4j"),
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion
  )
).dependsOn(
  scioCore,
  scioTest % "it"
).configs(IntegrationTest)

lazy val scioElasticsearch2: Project = Project(
  "scio-elasticsearch2",
  file("scio-elasticsearch2")
).settings(
  commonSettings,
  description := "Scio add-on for writing to Elasticsearch",
  libraryDependencies ++= Seq(
    "joda-time" % "joda-time" % jodaTimeVersion,
    "org.elasticsearch" % "elasticsearch" % elasticsearch2Version
  )
).dependsOn(
  scioCore,
  scioTest % "test"
)

lazy val scioElasticsearch5: Project = Project(
  "scio-elasticsearch5",
  file("scio-elasticsearch5")
).settings(
  commonSettings,
  description := "Scio add-on for writing to Elasticsearch",
  libraryDependencies ++= Seq(
    "joda-time" % "joda-time" % jodaTimeVersion,
    "org.elasticsearch.client" % "transport" % elasticsearch5Version
  )
).dependsOn(
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
  ).map(_ % circeVersion)
).dependsOn(
  scioCore,
  scioTest % "it,test->test"
).configs(IntegrationTest)

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
).dependsOn(
  scioCore,
  scioTest % "test"
)

lazy val scioParquet: Project = Project(
  "scio-parquet",
  file("scio-parquet")
).settings(
  commonSettings,
  description := "Scio add-on for Parquet",
  libraryDependencies ++= Seq(
    "me.lyh" %% "parquet-avro-extra" % parquetAvroExtraVersion,
    "com.google.cloud.bigdataoss" % "gcs-connector" % gcsConnectorVersion,
    "org.apache.beam" % "beam-sdks-java-io-hadoop-input-format" % beamVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
    "org.apache.parquet" % "parquet-avro" % parquetVersion
  )
).dependsOn(
  scioCore,
  scioSchemas % "test",
  scioTest % "test->test"
)

lazy val scioTensorFlow: Project = Project(
  "scio-tensorflow",
  file("scio-tensorflow")
).settings(
  commonSettings,
  description := "Scio add-on for TensorFlow",
  libraryDependencies ++= Seq(
    "org.tensorflow" % "tensorflow" % tensorFlowVersion,
    "org.tensorflow" % "proto" % tensorFlowVersion,
    "me.lyh" %% "shapeless-datatype-tensorflow" % shapelessDatatypeVersion
  ),
  libraryDependencies ++= Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)
).dependsOn(
  scioCore,
  scioTest % "test->test"
)

lazy val scioSchemas: Project = Project(
  "scio-schemas",
  file("scio-schemas")
).enablePlugins(ProtobufPlugin).settings(
  commonSettings ++ noPublishSettings,
  description := "Avro/Proto schemas for testing",
  version in AvroConfig := avroVersion,
  version in ProtobufConfig := protobufVersion,
  protobufRunProtoc in ProtobufConfig := (args =>
    // protoc-jar does not include 3.3.1 binary
    com.github.os72.protocjar.Protoc.runProtoc("-v3.3.0" +: args.toArray)
  ),
  // Avro and Protobuf files are compiled to src_managed/main/compiled_{avro,protobuf}
  // Exclude their parent to avoid confusing IntelliJ
  sourceDirectories in Compile := (sourceDirectories in Compile).value.filterNot(_.getPath.endsWith("/src_managed/main")),
  managedSourceDirectories in Compile := (managedSourceDirectories in Compile).value.filterNot(_.getPath.endsWith("/src_managed/main")),
  sources in doc in Compile := List(),  // suppress warnings
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
    "mysql" % "mysql-connector-java" % "5.1.+",
    "com.google.cloud.sql" % "mysql-socket-factory" % "1.0.2",
    "org.slf4j" % "slf4j-simple" % slf4jVersion,
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
    "org.mockito" % "mockito-all" % mockitoVersion % "test"
  ),
  addCompilerPlugin(paradiseDependency),
  sources in doc in Compile := List()
).dependsOn(
  scioCore,
  scioBigtable,
  scioSchemas,
  scioJdbc,
  scioExtra,
  scioTensorFlow,
  scioTest % "test"
)

lazy val scioRepl: Project = Project(
  "scio-repl",
  file("scio-repl")
).settings(
  commonSettings,
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
).dependsOn(
  scioCore,
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
    "org.slf4j" % "slf4j-nop" % slf4jVersion
  )
).dependsOn(
  scioCore
).enablePlugins(JmhPlugin)

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

lazy val siteSettings = Seq(
  autoAPIMappings := true,
  siteSubdirName in ScalaUnidoc := "api",
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
  gitRemoteRepo := "git@github.com:spotify/scio.git",
  mappings in makeSite ++= Seq(
    file("site/index.html") -> "index.html",
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
  ("com.google.apis", "google-api-services-bigquery", "https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest"),
  ("com.google.apis", "google-api-services-dataflow", "https://developers.google.com/resources/api-libraries/documentation/dataflow/v1b3/java/latest"),
  // FIXME: investigate why joda-time won't link
  ("joda-time", "joda-time", "http://www.joda.org/joda-time/apidocs"),
  ("org.apache.avro", "avro", "https://avro.apache.org/docs/current/api/java"),
  ("org.tensorflow", "libtensorflow", "https://www.tensorflow.org/api_docs/java/reference"))
val scalaMappings = Seq(
  ("com.twitter", "algebird-core", "https://twitter.github.io/algebird/api"),
  ("org.scalanlp", "breeze", "http://www.scalanlp.org/api/breeze"),
  ("org.scalatest", "scalatest", "http://doc.scalatest.org/3.0.0"))
val docMappings = javaMappings ++ scalaMappings
