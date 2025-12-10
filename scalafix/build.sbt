lazy val V = _root_.scalafix.sbt.BuildInfo

inThisBuild(
  List(
    resolvers += Resolver.sonatypeCentralSnapshots,
    organization := "com.spotify",
    scalaVersion := V.scala212,
    scalacOptions ++= List("-Yrangepos"),
    publish / skip := true,
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    semanticdbIncludeInJar := true,
    scalafmtOnCompile := false,
    scalafmtConfig := baseDirectory.value / ".." / ".scalafmt.conf",
    // discard scala-xml conflict for old scio releases
    libraryDependencySchemes ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % "always"
    )
  )
)

lazy val root = project
  .in(file("."))
  .aggregate(
    tests.projectRefs ++
      scalafixProjects.values.flatMap(p => Seq[sbt.ProjectReference](p.input, p.output)) ++
      Seq[sbt.ProjectReference](rules): _*
  )

lazy val rules = project
  .settings(
    moduleName := "scalafix",
    libraryDependencies ++= Seq(
      "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion
    )
  )

import ScalafixProject._
lazy val tests = projectMatrix
  .in(file("tests"))
  .enablePlugins(ScalafixTestkitPlugin)
  .scalafixRows(scalafixProjects.values.toList, rules)

def scio(version: String): List[ModuleID] = {
  val modules = List(
    "scio-core",
    "scio-avro",
    "scio-parquet",
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
        "scio-smb"
      )
  })

  modules.map("com.spotify" %% _ % version)
}

def scalafix(target: String, inputScio: String, outputScio: String): (String, ScalafixProject) = {
  val under = target.replace(".", "_")
  def p(prefix: String, dep: String) = sbt.Project
    .apply(s"$prefix-$under", file(s"$prefix-$under")).settings(libraryDependencies ++= scio(dep))
  target -> ScalafixProject(p("input", inputScio), p("output", outputScio), ConfigAxis(s"-scio-$under", s"-$under-"))
}

lazy val scalafixProjects = Map(
  scalafix("0.7", Scio.`0.6`, Scio.`0.7`),    // Scio 0.7
  scalafix("0.8", Scio.`0.7`, Scio.`0.8`),    // Scio 0.8
  scalafix("0.10", Scio.`0.9`, Scio.`0.11`),  // Scio 0.10
  scalafix("0.12", Scio.`0.11`, Scio.`0.12`), // Scio 0.12
  scalafix("0.13", Scio.`0.12`, Scio.`0.13`), // Scio 0.13
  scalafix("0.14", Scio.`0.13`, Scio.`0.14`), // Scio 0.14
  scalafix("0.15", Scio.`0.14`, Scio.`0.15`), // Scio 0.15
)

lazy val `input-0_7` = scalafixProjects("0.7").input
lazy val `output-0_7` = scalafixProjects("0.7").output
lazy val `input-0_8` = scalafixProjects("0.8").input
lazy val `output-0_8` = scalafixProjects("0.8").output
lazy val `input-0_10` = scalafixProjects("0.10").input
lazy val `output-0_10` = scalafixProjects("0.10").output
lazy val `input-0_12` = scalafixProjects("0.12").input
lazy val `output-0_12` = scalafixProjects("0.12").output
lazy val `input-0_13` = scalafixProjects("0.13").input
lazy val `output-0_13` = scalafixProjects("0.13").output
lazy val `input-0_14` = scalafixProjects("0.14").input
lazy val `output-0_14` = scalafixProjects("0.14").output
lazy val `input-0_15` = scalafixProjects("0.15").input
lazy val `output-0_15` = scalafixProjects("0.15").output
