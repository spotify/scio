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
import sbtunidoc.Plugin.UnidocKeys._
import com.trueaccord.scalapb.{ScalaPbPlugin => PB}

val dataflowSdkVersion = "1.7.0"
val algebirdVersion = "0.12.2"
val avroVersion = "1.7.7"
val bigQueryVersion = "v2-rev317-1.22.0"
val bigtableVersion = "0.9.2"
val breezeVersion ="0.12"
val chillVersion = "0.8.0"
val commonsIoVersion = "2.5"
val commonsMath3Version = "3.6.1"
val csvVersion = "0.1.12"
val guavaVersion = "19.0"
val hadoopVersion = "2.7.2"
val hamcrestVersion = "1.3"
val hbaseVersion = "1.0.2"
val javaLshVersion = "0.10"
val jodaConvertVersion = "1.8.1"
val junitVersion = "4.12"
val junitInterfaceVersion = "0.11"
val nettyTcNativeVersion = "1.1.33.Fork18"
val scalaCheckVersion = "1.13.2"
val scalaMacrosVersion = "2.1.0"
val scalapbVersion = "0.5.19" // inner protobuf-java version must match beam/dataflow-sdk one
val scalaTestVersion = "3.0.0"
val slf4jVersion = "1.7.21"

val java8 = sys.props("java.version").startsWith("1.8.")

val commonSettings = Sonatype.sonatypeSettings ++ assemblySettings ++ Seq(
  organization       := "com.spotify",

  scalaVersion       := "2.11.8",
  crossScalaVersions := Seq("2.10.6", "2.11.8"),
  scalacOptions                   ++= Seq("-target:jvm-1.7", "-deprecation", "-feature", "-unchecked"),
  scalacOptions in (Compile, doc) ++= Seq("-groups", "-skip-packages", "com.google"),
  javacOptions                    ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint:unchecked"),
  javacOptions in (Compile, doc)  := Seq("-source", "1.7"),

  testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),

  coverageExcludedPackages := Seq(
    "com\\.spotify\\.scio\\.examples\\..*",
    "com\\.spotify\\.scio\\.repl\\..*",
    "com\\.spotify\\.scio\\.util\\.MultiJoin"
  ).mkString(";"),
  coverageHighlighting := (if (scalaBinaryVersion.value == "2.10") false else true),

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
        <url>https://github.com/andrewsmartin</url>
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

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val assemblySettings = Seq(
  test in assembly := {},
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
    case s if s.endsWith("pom.xml") => MergeStrategy.last
    case s if s.endsWith(".class") => MergeStrategy.last
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
lazy val dataflowSdkDependency =
  "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % dataflowSdkVersion

lazy val root: Project = Project(
  "scio",
  file(".")
).settings(
  commonSettings ++ siteSettings ++ noPublishSettings,
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject
    -- inProjects(scioRepl) -- inProjects(scioSchemas) -- inProjects(scioExamples),
  run <<= run in Compile in scioRepl dependsOn sbtReplScalaVersionCheck,
  aggregate in assembly := false
).aggregate(
  scioCore,
  scioTest,
  scioBigQuery,
  scioBigtable,
  scioExtra,
  scioHdfs,
  scioRepl,
  scioExamples,
  scioSchemas
)

lazy val scioCore: Project = Project(
  "scio-core",
  file("scio-core")
).settings(
  commonSettings,
  description := "Scio - A Scala API for Google Cloud Dataflow",
  libraryDependencies ++= Seq(
    dataflowSdkDependency,
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter" % "chill-protobuf" % chillVersion,
    "commons-io" % "commons-io" % commonsIoVersion,
    "org.apache.commons" % "commons-math3" % commonsMath3Version
  )
).dependsOn(
  scioBigQuery
)

lazy val scioTest: Project = Project(
  "scio-test",
  file("scio-test")
).settings(
  commonSettings,
  description := "Scio helpers for ScalaTest",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion,
    // DataFlow testing requires junit and hamcrest
    "junit" % "junit" % junitVersion,
    "com.novocode" % "junit-interface" % junitInterfaceVersion,
    "org.hamcrest" % "hamcrest-all" % hamcrestVersion
  )
).dependsOn(
  scioCore,
  scioSchemas % "test"
)

lazy val scioBigQuery: Project = Project(
  "scio-bigquery",
  file("scio-bigquery")
).settings(
  commonSettings ++ Defaults.itSettings,
  description := "Scio add-on for Google BigQuery",
  libraryDependencies ++= Seq(
    dataflowSdkDependency,
    "com.google.apis" % "google-api-services-bigquery" % bigQueryVersion,
    "commons-io" % "commons-io" % commonsIoVersion,
    "org.joda" % "joda-convert" % jodaConvertVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-simple" % slf4jVersion % "test,it",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test,it"
  ),
  libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
  libraryDependencies ++= (
    if (scalaBinaryVersion.value == "2.10")
      List("org.scalamacros" %% "quasiquotes" % scalaMacrosVersion cross CrossVersion.binary)
    else
      Nil
  ),
  addCompilerPlugin(paradiseDependency)
).configs(IntegrationTest)

lazy val scioBigtable: Project = Project(
  "scio-bigtable",
  file("scio-bigtable")
).settings(
  commonSettings,
  description := "Scio add-on for Google Cloud Bigtable",
  libraryDependencies ++= Seq(
    "com.google.cloud.bigtable" % "bigtable-hbase-dataflow" % bigtableVersion exclude ("org.slf4j", "slf4j-log4j12"),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude ("org.slf4j", "slf4j-log4j12"),
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "io.netty" % "netty-tcnative-boringssl-static" % nettyTcNativeVersion
  )
).dependsOn(
  scioCore
)

lazy val scioExtra: Project = Project(
  "scio-extra",
  file("scio-extra")
).settings(
  commonSettings,
  description := "Scio extra utilities",
  libraryDependencies ++= Seq(
    "com.google.guava" % "guava" % guavaVersion,
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "org.scalanlp" %% "breeze" % breezeVersion,
    "info.debatty" % "java-lsh" % javaLshVersion,
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
  )
)

lazy val scioHdfs: Project = Project(
  "scio-hdfs",
  file("scio-hdfs")
).settings(
  commonSettings,
  description := "Scio add-on for HDFS",
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro-mapred" % avroVersion classifier("hadoop2"),
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion exclude ("org.slf4j", "slf4j-log4j12")
  )
).dependsOn(
  scioCore,
  scioTest % "test",
  scioSchemas % "test"
)

lazy val scioSchemas: Project = Project(
  "scio-schemas",
  file("scio-schemas")
).settings(
  commonSettings ++ sbtavro.SbtAvro.avroSettings ++ noPublishSettings ++ PB.protobufSettings,
  description := "Avro/Proto schemas for testing",
  libraryDependencies ++= Seq(
    "com.github.os72" % "protoc-jar" % "3.0.0-b1"
  ),
  // suppress warnings
  sources in doc in Compile := List(),
  javacOptions := Seq("-source", "1.7", "-target", "1.7"),
  compileOrder := CompileOrder.JavaThenScala,
  PB.javaConversions in PB.protobufConfig := true,
  PB.grpc := false,
  PB.runProtoc in PB.protobufConfig := (args =>
    com.github.os72.protocjar.Protoc.runProtoc("-v300" +: args.toArray)
  )
)

lazy val scioExamples: Project = Project(
  "scio-examples",
  file("scio-examples")
).settings(
  commonSettings ++ noPublishSettings,
  libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-simple" % slf4jVersion,
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
  ),
  addCompilerPlugin(paradiseDependency),
  sources in doc in Compile := List(),
  javacOptions := {
    if (java8)
      Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked")
    else
      javacOptions.value
  },
  unmanagedSourceDirectories in Compile ++= {
    if (java8) Seq(baseDirectory.value / "src/main/java8") else Nil
  }
).dependsOn(
  scioCore,
  scioBigtable,
  scioSchemas,
  scioHdfs,
  scioTest % "test"
)

val sbtReplScalaVersionCheck = Def.task {
  if (scalaVersion.value.startsWith("2.10"))
    sys.error("\n\n\tERROR: Can't start Scio REPL in SBT for scala 2.10.x. " +
                "Upgrade to 2.11.x. or build REPL assembly jar.\n" +
                "\tMore info https://github.com/spotify/scio/wiki/Scio-REPL\n\n")
}

lazy val scioRepl: Project = Project(
  "scio-repl",
  file("scio-repl")
).settings(
  commonSettings,
  libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-simple" % slf4jVersion,
    "jline" % "jline" % scalaBinaryVersion.value,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "com.nrinaudo" %% "kantan.csv" % csvVersion,
    paradiseDependency
  ),
  libraryDependencies ++= (
    if (scalaBinaryVersion.value == "2.10")
      List("org.scala-lang" % "jline" % scalaVersion.value)
    else
      Nil
  ),
  run <<= run in Compile dependsOn sbtReplScalaVersionCheck,
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

lazy val fixJavaDocLinksTask = taskKey[Unit]("Fix JavaDoc links")

lazy val siteSettings = site.settings ++ ghpages.settings ++ unidocSettings ++ Seq(
  autoAPIMappings := true,
  site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), ""),
  gitRemoteRepo := "git@github.com:spotify/scio.git",
  fixJavaDocLinksTask := {
    val bases = javaMappings.map(m => m._3 + "/index.html")
    val t = (target in ScalaUnidoc).value
    (t ** "*.html").get.foreach { f =>
      val doc = fixJavaDocLinks(bases, IO.read(f))
      IO.write(f, doc)
    }
  },
  // Insert fixJavaDocLinksTask between ScalaUnidoc.doc and SbtSite.makeSite
  fixJavaDocLinksTask <<= fixJavaDocLinksTask dependsOn (doc in ScalaUnidoc),
  makeSite <<= makeSite dependsOn fixJavaDocLinksTask
)

// =======================================================================
// API mappings
// =======================================================================

val javaMappings = Seq(
  ("com.google.cloud.dataflow", "google-cloud-dataflow-java-sdk-all",
   "https://cloud.google.com/dataflow/java-sdk/JavaDoc"),
  ("com.google.apis", "google-api-services-bigquery",
   "https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest"),
  ("joda-time", "joda-time", "http://www.joda.org/joda-time/apidocs")
 )
val scalaMappings = Seq(
  ("com.twitter", "algebird-core", "http://twitter.github.io/algebird"))
val docMappings = javaMappings ++ scalaMappings
