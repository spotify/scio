import sbt.librarymanagement.VersionNumber.SemVer

lazy val V = _root_.scalafix.sbt.BuildInfo

inThisBuild(
  List(
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
    tests.projectRefs ++ Seq[sbt.ProjectReference](
      // 0.7
      `input-0_7`,
      `output-0_7`,
      // 0.8
      `input-0_8`,
      `output-0_8`,
      // 0.10
      `input-0_10`,
      `output-0_10`,
      // 0.12
      `input-0_12`,
      `output-0_12`,
      // 0.13
      `input-0_13`,
      `output-0_13`,
      // scalafix
      rules
    ): _*
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
        "scio-extra" // new in 0.10
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

lazy val `input-0_13` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.12`)
  )

lazy val `output-0_13` = project
  .settings(
    libraryDependencies ++= scio(Scio.`0.13`)
  )

lazy val scio0_7 = ConfigAxis("-scio-0_7", "-0_7-")
lazy val scio0_8 = ConfigAxis("-scio-0_8", "-0_8-")
lazy val scio0_10 = ConfigAxis("-scio-0_10", "-0_10-")
lazy val scio0_12 = ConfigAxis("-scio-0_12", "-0_12-")
lazy val scio0_13 = ConfigAxis("-scio-0_13", "-0_13-")

lazy val tests = projectMatrix
  .in(file("tests"))
  .enablePlugins(ScalafixTestkitPlugin)
  .customRow(
    scalaVersions = Seq(V.scala212),
    axisValues = Seq(scio0_7, VirtualAxis.jvm),
    _.settings(
      moduleName := name.value + scio0_7.idSuffix,
      scalafixTestkitOutputSourceDirectories := (`output-0_7` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputSourceDirectories := (`input-0_7` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputClasspath := (`input-0_7` / Compile / fullClasspath).value
    ).dependsOn(rules)
  )
  .customRow(
    scalaVersions = Seq(V.scala212),
    axisValues = Seq(scio0_8, VirtualAxis.jvm),
    _.settings(
      moduleName := name.value + scio0_8.idSuffix,
      scalafixTestkitOutputSourceDirectories := (`output-0_8` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputSourceDirectories := (`input-0_8` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputClasspath := (`input-0_8` / Compile / fullClasspath).value
    ).dependsOn(rules)
  )
  .customRow(
    scalaVersions = Seq(V.scala212),
    axisValues = Seq(scio0_10, VirtualAxis.jvm),
    _.settings(
      moduleName := name.value + scio0_10.idSuffix,
      scalafixTestkitOutputSourceDirectories := (`output-0_10` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputSourceDirectories := (`input-0_10` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputClasspath := (`input-0_10` / Compile / fullClasspath).value
    ).dependsOn(rules)
  )
  .customRow(
    scalaVersions = Seq(V.scala212),
    axisValues = Seq(scio0_12, VirtualAxis.jvm),
    _.settings(
      moduleName := name.value + scio0_12.idSuffix,
      scalafixTestkitOutputSourceDirectories := (`output-0_12` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputSourceDirectories := (`input-0_12` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputClasspath := (`input-0_12` / Compile / fullClasspath).value
    ).dependsOn(rules)
  )
  .customRow(
    scalaVersions = Seq(V.scala212),
    axisValues = Seq(scio0_13, VirtualAxis.jvm),
    _.settings(
      moduleName := name.value + scio0_13.idSuffix,
      scalafixTestkitOutputSourceDirectories := (`output-0_13` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputSourceDirectories := (`input-0_13` / Compile / unmanagedSourceDirectories).value,
      scalafixTestkitInputClasspath := (`input-0_13` / Compile / fullClasspath).value
    ).dependsOn(rules)
  )
