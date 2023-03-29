import sbt.librarymanagement.VersionNumber.SemVer

lazy val V = _root_.scalafix.sbt.BuildInfo

inThisBuild(
  List(
    organization := "com.spotify",
    scalaVersion := V.scala212,
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= List("-Yrangepos"),
    publish / skip := true,
    scalafmtOnCompile := false,
    scalafmtConfig := baseDirectory.value / ".." / ".scalafmt.conf",
    // discard scala-xml conflict for old scio releases
    libraryDependencySchemes ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % "always"
    )
  )
)

lazy val rules = project
  .settings(
    moduleName := "scalafix",
    libraryDependencies ++= Seq(
      "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion
    )
  )

def scio(version: String): List[ModuleID] = {
  val modules = List(
    "scio-core",
    "scio-avro",
    "scio-test",
    "scio-jdbc",
    "scio-tensorflow"
  ) ++ (VersionNumber(version).numbers match {
    case Seq(0, minor, _) if minor < 10 =>
      List(
        "scio-bigquery"
      )
    case _ =>
      List(
        "scio-google-cloud-platform", // replaced scio-bigquery
        "scio-extra", // new in 0.10
      )
  })

  modules.map("com.spotify" %% _ % version)
}

lazy val `input-0_7` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.6`)
  )

lazy val `output-0_7` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.7`)
  )

lazy val `input-0_8` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.7`)
  )

lazy val `output-0_8` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.8`)
  )

lazy val `input-0_10` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.9`)
  )

lazy val `output-0_10` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.11`)
  )

lazy val `input-0_12` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.11`)
  )

lazy val `output-0_12` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.12`)
  )

lazy val tests = project
  .enablePlugins(ScalafixTestkitPlugin)
  .dependsOn(rules)
  .settings(
    libraryDependencies += "ch.epfl.scala" % "scalafix-testkit" % V.scalafixVersion % Test cross CrossVersion.full,
    Compile / compile := (Compile / compile).dependsOn(
      `input-0_7` / Compile / compile,
      `input-0_8` / Compile / compile,
      `input-0_10` / Compile / compile,
      `input-0_12` / Compile / compile
    ).value,
    scalafixTestkitOutputSourceDirectories :=
      (`output-0_7` / Compile / sourceDirectories).value ++
        (`output-0_8` / Compile / sourceDirectories).value ++
        (`output-0_10` / Compile / sourceDirectories).value ++
        (`output-0_12` / Compile / sourceDirectories).value,
    scalafixTestkitInputSourceDirectories :=
      (`input-0_7` / Compile / sourceDirectories).value ++
        (`input-0_8` / Compile / sourceDirectories).value ++
        (`input-0_10` / Compile / sourceDirectories).value ++
        (`input-0_12` / Compile / sourceDirectories).value,
    scalafixTestkitInputClasspath :=
      (`input-0_7` / Compile / fullClasspath).value ++
        (`input-0_8` / Compile / fullClasspath).value ++
        (`input-0_10` / Compile / fullClasspath).value ++
        (`input-0_12` / Compile / fullClasspath).value,
  )