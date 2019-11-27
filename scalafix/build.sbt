lazy val V = _root_.scalafix.sbt.BuildInfo
inThisBuild(
  List(
    organization := "com.spotify",
    scalaVersion := V.scala212,
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= List(
      "-Yrangepos"
    ),
    skip in publish := true
  )
)

lazy val scalafmtSettings = Seq(
  scalafmtConfig := baseDirectory.value / "../../.scalafmt.conf",
  scalafmtOnCompile := false
)

scalafmtConfig := baseDirectory.value / "../.scalafmt.conf"

lazy val rules = project
  .settings(
    moduleName := "scalafix",
    libraryDependencies += "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion
  )
  .settings(scalafmtSettings)

def scio(version: String) =
  List(
    "com.spotify" %% "scio-core",
    "com.spotify" %% "scio-avro",
    "com.spotify" %% "scio-bigquery",
    "com.spotify" %% "scio-test",
    "com.spotify" %% "scio-jdbc",
    "com.spotify" %% "scio-tensorflow"
  ).map(_ % version)

lazy val input = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.6`)
  )
  .settings(scalafmtSettings)

lazy val output = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.7`)
  )
  .settings(scalafmtSettings)

lazy val `input-0_8` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.7`)
  )
  .settings(scalafmtSettings)

lazy val `output-0_8` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.8`)
  )
  .settings(scalafmtSettings)

lazy val tests = project
  .settings(
    libraryDependencies += "ch.epfl.scala" % "scalafix-testkit" % V.scalafixVersion % Test cross CrossVersion.full,
    compile.in(Compile) :=
      compile
        .in(Compile)
        .dependsOn(
          compile.in(input, Compile),
          compile.in(`input-0_8`, Compile)
        )
        .value,
    scalafixTestkitOutputSourceDirectories :=
      sourceDirectories.in(output, Compile).value ++
        sourceDirectories.in(`output-0_8`, Compile).value,
    scalafixTestkitInputSourceDirectories :=
      sourceDirectories.in(`input-0_8`, Compile).value ++
        sourceDirectories.in(input, Compile).value,
    scalafixTestkitInputClasspath :=
      fullClasspath.in(input, Compile).value ++
        fullClasspath.in(`input-0_8`, Compile).value
  )
  .settings(scalafmtSettings)
  .dependsOn(rules)
  .enablePlugins(ScalafixTestkitPlugin)
