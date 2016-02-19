/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import com.typesafe.sbt.SbtSite.SiteKeys._
import com.typesafe.sbt.SbtSite.site
import com.typesafe.sbt.SbtGhPages.ghpages
import com.typesafe.sbt.SbtGit.GitKeys.gitRemoteRepo
import sbtunidoc.Plugin._
import sbtunidoc.Plugin.UnidocKeys._
import xerial.sbt.Sonatype
import xerial.sbt.Sonatype.SonatypeKeys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Sonatype.sonatypeSettings ++ Seq(
    organization       := "com.spotify",
    version            := "0.1.0-SNAPSHOT",

    scalaVersion       := "2.11.7",
    crossScalaVersions := Seq("2.10.6", "2.11.7"),
    scalacOptions      ++= Seq("-target:jvm-1.7", "-deprecation", "-feature", "-unchecked"),
    scalacOptions in (Compile, doc)    ++= Seq("-groups", "-skip-packages", "com.google"),
    javacOptions in (Compile, compile) ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint:unchecked"),

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
      ApiMappings.mappings.map(mappinngFn.tupled).flatten.toMap
    }
  )
}

object ScioBuild extends Build {
  import BuildSettings._
  import SiteSettings._

  val sdkVersion = "1.4.0"

  val macrosVersion = "2.0.1"
  val scalaTestVersion = "2.2.6"

  lazy val paradiseDependency =
    "org.scalamacros" % "paradise" % macrosVersion cross CrossVersion.full
  lazy val dataflowSdkDependency =
    "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % sdkVersion

  lazy val root: Project = Project(
    "scio",
    file("."),
    settings = buildSettings ++ siteSettings ++ Seq(run <<= run in Compile in scioExamples)
  ).settings(
    publish := {},
    publishLocal := {},
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject
      -- inProjects(scioSchemas) -- inProjects(scioExamples)
  ).aggregate(
    scioCore,
    scioTest,
    scioBigQuery,
    scioBigTable,
    scioExtra,
    scioHdfs,
    scioSchemas
  )

  lazy val scioCore: Project = Project(
    "scio-core",
    file("core"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        dataflowSdkDependency,
        "com.twitter" %% "algebird-core" % "0.11.0",
        "com.twitter" %% "bijection-avro" % "0.8.1",
        "com.twitter" %% "chill" % "0.7.2",
        "com.twitter" %% "chill-avro" % "0.7.2",
        "commons-io" % "commons-io" % "2.4",
        "org.apache.commons" % "commons-math3" % "3.6"
      )
    )
  ).dependsOn(
    scioBigQuery
  )

  lazy val scioTest: Project = Project(
    "scio-test",
    file("test"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % scalaTestVersion,
        // DataFlow testing requires junit and hamcrest
        "junit" % "junit" % "4.12",
        "org.hamcrest" % "hamcrest-all" % "1.3"
      )
    )
  ).dependsOn(
    scioCore,
    scioSchemas % "test"
  )

  lazy val scioBigQuery: Project = Project(
    "scio-bigquery",
    file("bigquery"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        dataflowSdkDependency,
        "commons-io" % "commons-io" % "2.4",
        "org.slf4j" % "slf4j-api" % "1.7.13",
        "joda-time" % "joda-time" % "2.7",
        "org.joda" % "joda-convert" % "1.7",
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
      ),
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
      libraryDependencies ++= (
        if (scalaVersion.value.startsWith("2.10"))
          List("org.scalamacros" %% "quasiquotes" % macrosVersion cross CrossVersion.binary)
        else
          Nil
      ),
      addCompilerPlugin(paradiseDependency)
    )
  )

  lazy val scioBigTable: Project = Project(
    "scio-bigtable",
    file("bigtable"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.cloud.bigtable" % "bigtable-hbase-dataflow" % "0.2.3" exclude ("org.slf4j", "slf4j-log4j12"),
        "org.apache.hadoop" % "hadoop-common" % "2.7.1" exclude ("org.slf4j", "slf4j-log4j12"),
        "org.apache.hbase" % "hbase-common" % "1.0.1"
      )
    )
  ).dependsOn(
    scioCore
  )

  lazy val scioExtra: Project = Project(
    "scio-extra",
    file("extra"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.guava" % "guava" % "19.0",
        "com.twitter" %% "algebird-core" % "0.11.0",
        "org.scalanlp" %% "breeze" % "0.12",
        "info.debatty" % "java-lsh" % "0.8",
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
      )
    )
  )

  lazy val scioHdfs: Project = Project(
    "scio-hdfs",
    file("hdfs"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.apache.avro" % "avro-mapred" % "1.7.7" classifier("hadoop2"),
        "org.apache.hadoop" % "hadoop-client" % "2.7.1" exclude ("org.slf4j", "slf4j-log4j12"),
        "junit" % "junit" % "4.12" % "test",
        "org.hamcrest" % "hamcrest-all" % "1.3" % "test"
      )
    )
  ).dependsOn(
    scioCore
  )

  lazy val scioSchemas: Project = Project(
    "scio-schemas",
    file("schemas"),
    settings = buildSettings ++ sbtavro.SbtAvro.avroSettings
  ).settings(
    publish := {},
    publishLocal := {}
  )

  lazy val scioExamples: Project = Project(
    "scio-examples",
    file("examples"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-simple" % "1.7.13"
      ),
      addCompilerPlugin(paradiseDependency)
    )
  ).settings(
    publish := {},
    publishLocal := {}
  ).dependsOn(
    scioCore,
    scioBigTable,
    scioSchemas,
    scioTest % "test"
  )
}

object SiteSettings {
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

  val siteSettings = site.settings ++ ghpages.settings ++ unidocSettings ++ Seq(
    autoAPIMappings := true,
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), ""),
    gitRemoteRepo := "git@github.com:spotify/scio.git",
    fixJavaDocLinksTask := {
      val bases = ApiMappings.javaMappings.map(m => m._3 + "/index.html")
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
}

object ApiMappings {
  val javaMappings = Seq(
    ("com.google.cloud.dataflow", "google-cloud-dataflow-java-sdk-all",
     "https://cloud.google.com/dataflow/java-sdk/JavaDoc"),
    ("com.google.apis", "google-api-services-bigquery",
     "https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest"),
    ("joda-time", "joda-time", "http://www.joda.org/joda-time/apidocs")
   )
  val scalaMappings = Seq(
    ("com.twitter", "algebird-core", "http://twitter.github.io/algebird"))
  val mappings = javaMappings ++ scalaMappings
}
