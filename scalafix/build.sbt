lazy val V = _root_.scalafix.sbt.BuildInfo
inThisBuild(
  List(
    organization := "com.spotify",
    scalaVersion := V.scala212,
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= List(
      "-Yrangepos"
    ),
    skip in publish := true,
    scalafmtOnCompile := false,
    scalafmtConfig := baseDirectory.value / ".." / ".scalafmt.conf"
  )
)

lazy val rules = project
  .settings(
    moduleName := "scalafix",
    libraryDependencies += "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion
  )

def scio(version: String) =
  List(
    "com.spotify" %% "scio-core",
    "com.spotify" %% "scio-avro",
    "com.spotify" %% "scio-bigquery",
    "com.spotify" %% "scio-test",
    "com.spotify" %% "scio-jdbc",
    "com.spotify" %% "scio-tensorflow"
  ).map(_ % version)

def scio10(version: String) =
  List(
    "com.spotify" %% "scio-core",
    "com.spotify" %% "scio-avro",
    "com.spotify" %% "scio-google-cloud-platform",
    "com.spotify" %% "scio-test",
    "com.spotify" %% "scio-jdbc",
    "com.spotify" %% "scio-tensorflow"
  ).map(_ % version)

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
    libraryDependencies ++= scio10(Scio.`0.10`)
  )

lazy val tests = project
  .settings(
    libraryDependencies += "ch.epfl.scala" % "scalafix-testkit" % V.scalafixVersion % Test cross CrossVersion.full,
    compile.in(Compile) :=
      compile
        .in(Compile)
        .dependsOn(
          compile.in(`input-0_7`, Compile),
          compile.in(`input-0_8`, Compile),
          compile.in(`input-0_10`, Compile)
        )
        .value,
    scalafixTestkitOutputSourceDirectories :=
      sourceDirectories.in(`output-0_7`, Compile).value ++
        sourceDirectories.in(`output-0_8`, Compile).value ++
        sourceDirectories.in(`output-0_10`, Compile).value,
    scalafixTestkitInputSourceDirectories :=
      sourceDirectories.in(`input-0_7`, Compile).value ++
        sourceDirectories.in(`input-0_8`, Compile).value ++
        sourceDirectories.in(`input-0_10`, Compile).value,
    scalafixTestkitInputClasspath :=
      fullClasspath.in(`input-0_7`, Compile).value ++
        fullClasspath.in(`input-0_8`, Compile).value ++
        fullClasspath.in(`input-0_10`, Compile).value
  )
  .dependsOn(rules)
  .enablePlugins(ScalafixTestkitPlugin)
