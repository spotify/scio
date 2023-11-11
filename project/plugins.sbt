addDependencyTreePlugin
addSbtPlugin("ch.epfl.scala" % "sbt-bloop" % "1.5.11")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.4")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("com.github.sbt" % "sbt-avro" % "3.4.3")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.10.5")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")
addSbtPlugin("com.github.sbt" % "sbt-ghpages" % "0.8.0")
addSbtPlugin("com.github.sbt" % "sbt-site" % "1.5.0")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.5.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.4.4")
addSbtPlugin("io.github.jonas" % "sbt-paradox-material-theme" % "0.6.0")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.5.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.9")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.6")

libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.8.2"

// force usage of scala-xml v2
// See https://github.com/scoverage/sbt-scoverage/issues/439
dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
