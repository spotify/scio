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
import com.typesafe.sbt.SbtSite.SiteKeys._
import com.typesafe.sbt.SbtGit.GitKeys.gitRemoteRepo

val beamVersion = "2.1.0"

val algebirdVersion = "0.13.0"
val annoy4sVersion = "0.5.0"
val annoyVersion = "0.2.5"
val asmVersion = "4.5"
val autoServiceVersion = "1.0-rc3"
val autoValueVersion = "1.4.1"
val avroVersion = "1.8.1"
val bigtableVersion = "1.0.0-pre2"
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
val kantanCsvVersion = "0.2.1"
val kryoVersion = "4.0.1" // explicitly depend on 4.0.1 due to https://github.com/EsotericSoftware/kryo/pull/516
val mockitoVersion = "1.10.19"
val parquetAvroExtraVersion = "0.2.2"
val parquetVersion = "1.9.0"
val protobufGenericVersion = "0.2.2"
val protobufVersion = "3.3.1"
val scalacheckShapelessVersion = "1.1.6"
val scalacheckVersion = "1.13.5"
val scalaMacrosVersion = "2.1.0"
val scalatestVersion = "3.0.3"
val shapelessDatatypeVersion = "0.1.7"
val slf4jVersion = "1.7.25"
val sparkeyVersion = "2.1.3"
val tensorFlowVersion = "1.3.0"

val commonSettings = Sonatype.sonatypeSettings ++ assemblySettings ++ Seq(
  organization       := "com.spotify",

  scalaVersion       := "2.12.3",
  crossScalaVersions := Seq("2.11.11", "2.12.3"),
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

  coverageExcludedPackages := Seq(
    "com\\.spotify\\.scio\\.examples\\..*",
    "com\\.spotify\\.scio\\.repl\\..*",
    "com\\.spotify\\.scio\\.util\\.MultiJoin"
  ).mkString(";"),
  coverageHighlighting := true,

  // Release settings
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  sonatypeProfileName           := "com.spotify",
  pomExtra                      := {
    <url>https://github.com/spotify/scio</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>git@github.com/spotify/scio.git</url>
      <connection>scm:git:git@github.com:spotify/scio.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sinisa_lyh</id>
        <name>Neville Li</name>
        <url>https://twitter.com/sinisa_lyh</url>
      </developer>
      <developer>
        <id>ravwojdyla</id>
        <name>Rafal Wojdyla</name>
        <url>https://twitter.com/ravwojdyla</url>
      </developer>
      <developer>
        <id>andrewsmartin</id>
        <name>Andrew Martin</name>
        <url>https://twitter.com/andrew_martin92</url>
      </developer>
    </developers>
  },

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
    val mappinngFn = (organization: String, name: String, apiUrl: String) => {
      (for {
        entry <- (fullClasspath in Compile).value
        module <- entry.get(moduleID.key)
        if module.organization == organization
        if module.name.startsWith(name)
          jarFile = entry.data
      } yield jarFile).headOption.map((_, url(apiUrl)))
    }
    docMappings.map(mappinngFn.tupled).flatten.toMap
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
lazy val beamDependencies = Seq(
  "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
)

lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)

lazy val root: Project = Project(
  "scio",
  file(".")
).enablePlugins(GhpagesPlugin, ScalaUnidocPlugin, SiteScaladocPlugin).settings(
  commonSettings ++ siteSettings ++ noPublishSettings,
  scalacOptions in (ScalaUnidoc, unidoc) += "-Ymacro-no-expand",
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject
    -- inProjects(scioCassandra2) -- inProjects(scioElasticsearch2)
    -- inProjects(scioRepl) -- inProjects(scioSchemas) -- inProjects(scioExamples),
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
  scioRepl
)

lazy val scioCore: Project = Project(
  "scio-core",
  file("scio-core")
).settings(
  commonSettings ++ macroSettings,
  description := "Scio - A Scala API for Apache Beam and Google Cloud Dataflow",
  libraryDependencies ++= beamDependencies,
  libraryDependencies ++= Seq(
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
    "com.google.auto.value" % "auto-value" % autoValueVersion % "provided",
    "com.google.cloud.bigtable" % "bigtable-client-core" % bigtableVersion,
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
    "org.tensorflow" % "proto" % tensorFlowVersion
  )
).dependsOn(
  scioCore,
  scioTest % "test->test"
)

lazy val scioSchemas: Project = Project(
  "scio-schemas",
  file("scio-schemas")
).settings(
  commonSettings ++ noPublishSettings,
  version in avroConfig := avroVersion, // Set avro version used by sbt-avro
  description := "Avro/Proto schemas for testing",
  // suppress warnings
  sources in doc in Compile := List(),
  // generate both Avro and Protobuf sources in src_managed/main to avoid confusing IntelliJ
  javaSource in avroConfig := (sourceManaged in Compile).value,
  // Avro and Protobuf
  compileOrder := CompileOrder.JavaThenScala,
  PB.targets in Compile := Seq(
    PB.gens.java -> (sourceManaged in Compile).value
  ),
  // avoid accidentally deleting Avro sources
  PB.deleteTargetDirectory := false
)

lazy val scioExamples: Project = Project(
  "scio-examples",
  file("scio-examples")
).settings(
  commonSettings ++ noPublishSettings,
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
  scioElasticsearch5,
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

lazy val siteSettings = Seq(
  autoAPIMappings := true,
  siteSubdirName in ScalaUnidoc := "",
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
  gitRemoteRepo := "git@github.com:spotify/scio.git",
  makeSite := {
    // fix JavaDoc links before makeSite
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
  // FIXME: investigate why joda-time won't link
  ("joda-time", "joda-time", "http://www.joda.org/joda-time/apidocs"),
  ("org.apache.avro", "avro", s"https://avro.apache.org/docs/$avroVersion/api/java"),
  ("org.tensorflow", "libtensorflow", "https://www.tensorflow.org/api_docs/java/reference"))
val scalaMappings = Seq(
  ("com.twitter", "algebird-core", "http://twitter.github.io/algebird/api"),
  ("org.scalanlp", "breeze", "http://www.scalanlp.org/api/breeze/"),
  ("org.scalatest", "scalatest", s"http://doc.scalatest.org/$scalatestVersion/"))
val docMappings = javaMappings ++ scalaMappings
