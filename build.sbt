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

val beamVersion = "0.5.0"
val algebirdVersion = "0.12.4"
val annoyVersion = "0.2.5"
val autoServiceVersion = "1.0-rc2"
val avroVersion = "1.7.7"
val breezeVersion ="0.13"
val chillVersion = "0.9.1"
val commonsIoVersion = "2.5"
val commonsMath3Version = "3.6.1"
val csvVersion = "0.1.16"
val guavaVersion = "19.0"
val hadoopVersion = "2.7.2"
val hamcrestVersion = "1.3"
val jacksonScalaModuleVersion = "2.8.6"
val javaLshVersion = "0.10"
val jodaConvertVersion = "1.8.1"
val jodaTimeVersion = "2.9.4"
val junitVersion = "4.12"
val junitInterfaceVersion = "0.11"
val mockitoVersion = "1.10.19"
val nettyTcNativeVersion = "1.1.33.Fork18"
val protobufGenericVersion = "0.2.0"
val scalaMacrosVersion = "2.1.0"
val scalaMeterVersion = "0.8.2"
val scalacheckShapelessVersion = "1.1.4"
val scalacheckVersion = "1.13.4"
val scalatestVersion = "3.0.1"
val shapelessDatatypeVersion = "0.1.2"
val slf4jVersion = "1.7.22"
val sparkeyVersion = "2.1.3"

val scalaMeterFramework = new TestFramework("org.scalameter.ScalaMeterFramework")

val commonSettings = Sonatype.sonatypeSettings ++ assemblySettings ++ Seq(
  organization       := "com.spotify",

  scalaVersion       := "2.11.8",
  crossScalaVersions := Seq("2.11.8"),
  scalacOptions                   ++= Seq("-Xmax-classfile-name", "100", "-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  scalacOptions in (Compile, doc) ++= Seq("-groups", "-skip-packages", "org.apache.beam"),
  javacOptions                    ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc)  := Seq("-source", "1.8"),

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
).settings(
  commonSettings ++ siteSettings ++ noPublishSettings,
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject
    -- inProjects(scioRepl) -- inProjects(scioSchemas) -- inProjects(scioExamples),
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
  commonSettings ++ macroSettings,
  description := "Scio - A Scala API for Google Cloud Dataflow",
    libraryDependencies ++= beamDependencies,
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter" %% "chill-algebird" % chillVersion,
    "com.twitter" % "chill-protobuf" % chillVersion,
    "commons-io" % "commons-io" % commonsIoVersion,
    "org.apache.commons" % "commons-math3" % commonsMath3Version,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonScalaModuleVersion,
    "com.google.auto.service" % "auto-service" % autoServiceVersion,
    "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
    "junit" % "junit" % junitVersion % "provided"
  )
).dependsOn(
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
    // DataFlow testing requires junit and hamcrest
    "org.hamcrest" % "hamcrest-all" % hamcrestVersion,
    "com.spotify" % "annoy" % annoyVersion % "test",
    "com.spotify.sparkey" % "sparkey" % sparkeyVersion % "test",
    "junit" % "junit" % junitVersion
  ),
  addCompilerPlugin(paradiseDependency)
).configs(
  IntegrationTest
).dependsOn(
  scioCore,
  scioSchemas % "test"
)

lazy val scioBigQuery: Project = Project(
  "scio-bigquery",
  file("scio-bigquery")
).settings(
  commonSettings ++ macroSettings ++ itSettings,
  description := "Scio add-on for Google BigQuery",
    libraryDependencies ++= beamDependencies,
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
  commonSettings,
  description := "Scio add-on for Google Cloud Bigtable",
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
    "io.netty" % "netty-tcnative-boringssl-static" % nettyTcNativeVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )
).dependsOn(
  scioCore
)

lazy val scioExtra: Project = Project(
  "scio-extra",
  file("scio-extra")
).settings(
  commonSettings ++ itSettings,
  description := "Scio extra utilities",
  libraryDependencies ++= Seq(
    "com.google.guava" % "guava" % guavaVersion,
    "com.spotify.sparkey" % "sparkey" % sparkeyVersion,
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "org.scalanlp" %% "breeze" % breezeVersion,
    "info.debatty" % "java-lsh" % javaLshVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test",
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
  )
).dependsOn(
  scioCore,
  scioTest % "it,test"
).configs(IntegrationTest)

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
  scioTest % "test->test",
  scioSchemas % "test"
)

lazy val scioSchemas: Project = Project(
  "scio-schemas",
  file("scio-schemas")
).settings(
  commonSettings ++ sbtavro.SbtAvro.avroSettings ++ noPublishSettings,
  version in avroConfig := avroVersion, // Set avro version used by sbt-avro
  description := "Avro/Proto schemas for testing",
  // suppress warnings
  sources in doc in Compile := List(),
  compileOrder := CompileOrder.JavaThenScala,
  PB.targets in Compile := Seq(
    PB.gens.java -> (sourceManaged in Compile).value / "compiled_protobuf",
    scalapb.gen(javaConversions=true, grpc=false) -> (sourceManaged in Compile).value / "compiled_protobuf"
  )
)

lazy val scioExamples: Project = Project(
  "scio-examples",
  file("scio-examples")
).settings(
  commonSettings ++ noPublishSettings,
  libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-simple" % slf4jVersion,
    "org.apache.beam" % "beam-sdks-java-core" % beamVersion % "test" classifier "tests",
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
    "org.mockito" % "mockito-all" % mockitoVersion % "test"
  ),
  addCompilerPlugin(paradiseDependency),
  sources in doc in Compile := List()
).dependsOn(
  scioCore,
  scioBigtable,
  scioSchemas,
  scioHdfs,
  scioTest % "test"
)

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
  assemblyJarName in assembly := s"scio-repl-${version.value}.jar"
).dependsOn(
  scioCore,
  scioExtra
)

lazy val scioBench: Project = Project(
  "scio-bench",
  file("scio-bench")
).settings(
  commonSettings ++ noPublishSettings,
  description := "Scio micro-benchmarks",
  libraryDependencies ++= Seq(
    "com.storm-enroute" %% "scalameter" % scalaMeterVersion % "test",
    "com.google.guava" % "guava" % guavaVersion % "test"
  ),
  testFrameworks += scalaMeterFramework,
  testOptions += Tests.Argument(scalaMeterFramework, "-silent"),
  parallelExecution in Test := false,
  logBuffered := false
).dependsOn(
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
  fixJavaDocLinksTask := fixJavaDocLinksTask dependsOn (doc in ScalaUnidoc),
  makeSite := (makeSite dependsOn fixJavaDocLinksTask).value
)

// =======================================================================
// API mappings
// =======================================================================

val javaMappings = Seq(
  ("org.apache.beam", "beam-sdks-java-core",
   s"https://beam.apache.org/documentation/sdks/javadoc/$beamVersion/"),
  ("org.apache.beam", "beam-runners-direct-java",
   s"https://beam.apache.org/documentation/sdks/javadoc/$beamVersion/"),
  ("org.apache.beam", "beam-runners-google-cloud-dataflow-java",
   s"https://beam.apache.org/documentation/sdks/javadoc/$beamVersion/"),
  ("org.apache.beam", "beam-sdks-java-io-google-cloud-platform",
   s"https://beam.apache.org/documentation/sdks/javadoc/$beamVersion/"),
  ("com.google.apis", "google-api-services-bigquery",
   "https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest"),
  ("joda-time", "joda-time", "http://www.joda.org/joda-time/apidocs")
 )
val scalaMappings = Seq(
  ("com.twitter", "algebird-core", "http://twitter.github.io/algebird"))
val docMappings = javaMappings ++ scalaMappings
