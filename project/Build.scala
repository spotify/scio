import sbt._
import Keys._
import xerial.sbt.Sonatype
import xerial.sbt.Sonatype.SonatypeKeys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Sonatype.sonatypeSettings ++ Seq(
    organization       := "com.spotify",
    version            := "0.1.0-SNAPSHOT",

    scalaVersion       := "2.10.5",
    crossScalaVersions := Seq("2.10.5", "2.11.6"),
    scalacOptions      ++= Seq(),
    javacOptions       ++= Seq("-source", "1.7", "-target", "1.7")
  )
}

object DataflowScalaBuild extends Build {
  import BuildSettings._

  val sdkVersion = "0.4.150414"

  val chillVersion = "0.5.2"
  val macrosVersion = "2.0.1"
  val scalaTestVersion = "2.2.1"

  lazy val root: Project = Project(
    "root",
    file("."),
    settings = buildSettings ++ Seq(
      run <<= run in Compile in dataflowScalaExamples)
  ).settings(
    publish := {},
    publishLocal := {}
  ).aggregate(
    dataflowScalaCore,
    dataflowScalaTest,
    bigqueryScala,
    dataflowScalaSchemas,
    dataflowScalaExamples
  )

  lazy val dataflowScalaCore: Project = Project(
    "dataflow-scala-core",
    file("core"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % sdkVersion,
        "com.twitter" %% "algebird-core" % "0.9.0",
        "com.twitter" %% "chill" % chillVersion,
        "com.twitter" %% "chill-avro" % chillVersion
      )
    )
  ).dependsOn(
    bigqueryScala,
    dataflowScalaSchemas % "test",
    dataflowScalaTest % "test"
  )

  lazy val dataflowScalaTest: Project = Project(
    "dataflow-scala-test",
    file("test"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % sdkVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersion,
        // DataFlow testing requires junit and hamcrest
        "junit" % "junit" % "4.12",
        "org.hamcrest" % "hamcrest-all" % "1.3"
      )
    )
  )

  lazy val bigqueryScala: Project = Project(
    "bigquery-scala",
    file("bigquery"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.apis" % "google-api-services-bigquery" % "v2-rev187-1.19.1"
          exclude("com.google.guava", "guava-jdk5"),
        "org.scalatest" %% "scalatest" % scalaTestVersion
      ),
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
      libraryDependencies := {
        CrossVersion.partialVersion(scalaVersion.value) match {
          // if Scala 2.11+ is used, quasiquotes are available in the standard distribution
          case Some((2, scalaMajor)) if scalaMajor >= 11 =>
            libraryDependencies.value
          // in Scala 2.10, quasiquotes are provided by macro paradise
          case Some((2, 10)) =>
            libraryDependencies.value ++ Seq(
              compilerPlugin("org.scalamacros" % "paradise" % macrosVersion cross CrossVersion.full),
              "org.scalamacros" %% "quasiquotes" % macrosVersion cross CrossVersion.binary)
        }
      }
    )
  )

  lazy val dataflowScalaSchemas: Project = Project(
    "dataflow-scala-schemas",
    file("schemas"),
    settings = buildSettings ++ sbtavro.SbtAvro.avroSettings
  ).settings(
    publish := {},
    publishLocal := {}
  )

  lazy val dataflowScalaExamples: Project = Project(
    "dataflow-scala-examples",
    file("examples"),
    settings = buildSettings
  ).settings(
    publish := {},
    publishLocal := {}
  ).dependsOn(
    dataflowScalaCore,
    dataflowScalaSchemas,
    dataflowScalaTest % "test"
  )
}
