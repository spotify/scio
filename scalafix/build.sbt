lazy val V = _root_.scalafix.sbt.BuildInfo
inThisBuild(
  List(
    organization := "com.spotify",
    scalaVersion := V.scala212,
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= List(
      "-Yrangepos"
    )
  )
)

skip in publish := true

lazy val rules = project.settings(
  moduleName := "scalafix",
  libraryDependencies += "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion
)

lazy val input = project.settings(
  skip in publish := true,
  libraryDependencies += "com.spotify" %% "scio-core" % Scio.`0.6`,
  libraryDependencies += "com.spotify" %% "scio-bigquery" % Scio.`0.6`,
  libraryDependencies += "com.spotify" %% "scio-test" % Scio.`0.6`
)

lazy val output = project.settings(
  skip in publish := true,
  libraryDependencies += "com.spotify" %% "scio-core" % Scio.`0.7`,
  libraryDependencies += "com.spotify" %% "scio-bigquery" % Scio.`0.7`,
  libraryDependencies += "com.spotify" %% "scio-test" % Scio.`0.7`
)

lazy val `input-0_8` =
  project.settings(
    skip in publish := true,
    libraryDependencies += "com.spotify" %% "scio-core" % Scio.`0.7`,
    libraryDependencies += "com.spotify" %% "scio-test" % Scio.`0.7`,
    libraryDependencies += "com.spotify" %% "scio-jdbc" % Scio.`0.7`
  )

lazy val `output-0_8` =
  project.settings(
    skip in publish := true,
    libraryDependencies += "com.spotify" %% "scio-core" % Scio.`0.8`,
    libraryDependencies += "com.spotify" %% "scio-test" % Scio.`0.8`,
    libraryDependencies += "com.spotify" %% "scio-jdbc" % Scio.`0.8`
  )

lazy val tests = project
  .settings(
    skip in publish := true,
    libraryDependencies += "ch.epfl.scala" % "scalafix-testkit" % V.scalafixVersion % Test cross CrossVersion.full,
    compile.in(Compile) :=
      compile.in(Compile).dependsOn(
        compile.in(input, Compile),
        compile.in(`input-0_8`, Compile)
      ).value,
    scalafixTestkitOutputSourceDirectories :=
      sourceDirectories.in(output, Compile).value ++
      sourceDirectories.in(`output-0_8`, Compile).value,
    scalafixTestkitInputSourceDirectories :=
      sourceDirectories.in(`input-0_8`, Compile).value ++
      sourceDirectories.in(input, Compile).value,
    scalafixTestkitInputClasspath :=
      fullClasspath.in(input, Compile).value ++
      fullClasspath.in(`input-0_8`, Compile).value,
  )
  .dependsOn(rules)
  .enablePlugins(ScalafixTestkitPlugin)
