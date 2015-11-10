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
    scalacOptions in (Compile,doc)     ++= Seq("-groups", "-skip-packages", "com.google"),
    javacOptions in (Compile)          ++= Seq("-source", "1.7"),
    javacOptions in (Compile, compile) ++= Seq("-target", "1.7"),

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

  val sdkVersion = "1.2.1"

  val guavaVersion = "18.0"
  val chillVersion = "0.7.2"
  val macrosVersion = "2.0.1"
  val scalaTestVersion = "2.2.5"

  lazy val paradiseDependency =
    "org.scalamacros" % "paradise" % macrosVersion cross CrossVersion.full
  lazy val bigQueryDependency =
    "com.google.apis" % "google-api-services-bigquery" % "v2-rev218-1.20.0" exclude ("com.google.guava", "guava-jdk5")
  lazy val dataflowSdkDependencies = Seq(
    ("com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % sdkVersion)
      .exclude("com.google.apis", "google-api-services-bigquery")
      .exclude("com.google.guava", "guava"),
    bigQueryDependency
  )

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
    scioSchemas,
    scioExamples
  )

  lazy val scioCore: Project = Project(
    "scio-core",
    file("core"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= dataflowSdkDependencies,
      libraryDependencies ++= Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.twitter" %% "algebird-core" % "0.11.0",
        "com.twitter" %% "chill" % chillVersion,
        "com.twitter" %% "chill-avro" % chillVersion,
        "commons-io" % "commons-io" % "2.4",
        "org.apache.commons" % "commons-math3" % "3.5"
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
        bigQueryDependency,
        "com.google.guava" % "guava" % guavaVersion,
        "org.slf4j" % "slf4j-api" % "1.7.7",
        "org.slf4j" % "slf4j-simple" % "1.7.7" % "provided",
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
        "com.google.cloud.bigtable" % "bigtable-hbase-dataflow" % "0.2.1",
        "org.apache.hadoop" % "hadoop-common" % "2.4.1",
        "org.apache.hbase" % "hbase-common" % "1.0.1"
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
  ).dependsOn(
    scioBigQuery
  )

  lazy val scioExamples: Project = Project(
    "scio-examples",
    file("examples"),
    settings = buildSettings ++ Seq(
      addCompilerPlugin(paradiseDependency)
    )
  ).settings(
    publish := {},
    publishLocal := {}
  ).dependsOn(
    scioCore,
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
