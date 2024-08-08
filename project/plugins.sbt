addDependencyTreePlugin
addSbtPlugin("org.typelevel" % "sbt-typelevel" % "0.7.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.12.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.12.0")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("com.github.sbt" % "sbt-avro" % "3.4.4")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.here.platform" % "sbt-bom" % "1.0.14")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.10.7")
addSbtPlugin("com.lightbend.sbt" % "sbt-bill-of-materials" % "1.0.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("com.github.sbt" % "sbt-ghpages" % "0.8.0")
addSbtPlugin("com.github.sbt" % "sbt-site" % "1.7.0")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.7.0")
addSbtPlugin("com.github.sbt" % "sbt-paradox-material-theme" % "0.7.0")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.5.4")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.1.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.7")

val avroVersion = sys.props.get("avro.version").getOrElse("1.11.3")
libraryDependencies ++= Seq(
  "org.apache.avro" % "avro-compiler" % avroVersion,
  "org.typelevel" %% "scalac-options" % "0.1.5"
)
