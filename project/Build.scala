import sbt._
import Keys._
import xerial.sbt.Sonatype
import xerial.sbt.Sonatype.SonatypeKeys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Sonatype.sonatypeSettings ++ Seq(
    organization       := "com.spotify.data",
    version            := "0.1.0-SNAPSHOT",

    scalaVersion       := "2.10.5",
    crossScalaVersions := Seq("2.10.5", "2.11.6"),
    scalacOptions      ++= Seq(),
    javacOptions       ++= Seq("-source", "1.7", "-target", "1.7")
  )
}

object DataflowScalaBuild extends Build {
  import BuildSettings._

  lazy val root: Project = Project(
    "root",
    file("."),
    settings = buildSettings ++ Seq(
      run <<= run in Compile in dataflowScalaExamples)
  ).settings(
    publish         := {},
    publishLocal    := {}
  ).aggregate(
    dataflowScalaCore,
    dataflowScalaExamples,
    dataflowScalaSchemas,
    dataflowScalaTest
  )

  lazy val dataflowScalaCore: Project = Project(
    "dataflow-scala-core",
    file("core"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "latest.integration",
        "com.twitter" %% "algebird-core" % "0.9.0",
        "com.twitter" %% "chill" % "0.5.2",
        "com.twitter" %% "chill-avro" % "0.5.2",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test",
        // DataFlow testing requires junit and hamcrest
        "junit" % "junit" % "4.12" % "test",
        "org.hamcrest" % "hamcrest-all" % "1.3" % "test"
      )
    )
  ).dependsOn(
    bigqueryScala,
    dataflowScalaSchemas % "test",
    dataflowScalaTest % "test"
  )

  lazy val bigqueryScala: Project = Project(
    "bigquery-scala",
    file("bigquery"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "latest.integration",
        "org.scalatest" %% "scalatest" % "2.2.1",
        // DataFlow testing requires junit and hamcrest
        "junit" % "junit" % "4.12",
        "org.hamcrest" % "hamcrest-all" % "1.3"
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
              compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full),
              "org.scalamacros" %% "quasiquotes" % "2.0.1" cross CrossVersion.binary)
        }
      }
    )
  )

  lazy val dataflowScalaSchemas: Project = Project(
    "dataflow-scala-schemas",
    file("schemas"),
    settings = buildSettings ++ sbtavro.SbtAvro.avroSettings
  ).settings(
    publish         := {},
    publishLocal    := {}
  )

  lazy val dataflowScalaTest: Project = Project(
    "dataflow-scala-test",
    file("test"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "latest.integration",
        "org.scalatest" %% "scalatest" % "2.2.1",
        // DataFlow testing requires junit and hamcrest
        "junit" % "junit" % "4.12",
        "org.hamcrest" % "hamcrest-all" % "1.3"
      )
    )
  )

  lazy val dataflowScalaExamples: Project = Project(
    "dataflow-scala-examples",
    file("examples"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "2.2.1" % "test",
        // DataFlow testing requires junit and hamcrest
        "junit" % "junit" % "4.12" % "test",
        "org.hamcrest" % "hamcrest-all" % "1.3" % "test"
      )
    )
  ).settings(
    publish         := {},
    publishLocal    := {}
  ).dependsOn(
    dataflowScalaCore,
    dataflowScalaSchemas,
    dataflowScalaTest % "test"
  )
}
