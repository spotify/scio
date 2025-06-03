import sbt._

object Exclude {
  val gcsio: ExclusionRule = "com.google.cloud.bigdataoss" % "gcsio"
  // replaced by io.dropwizard.metrics metrics-core
  val metricsCore: ExclusionRule = "com.codahale.metrics" % "metrics-core"
  // kafka isn't exposed in scio and pulling too many things
  val beamKafka: ExclusionRule = "org.apache.beam" % "beam-sdks-java-io-kafka"
  // logger implementation must be given by the runner lib
  val loggerImplementations: Seq[ExclusionRule] = Seq(
    "ch.qos.logback" % "logback-classic",
    "ch.qos.logback" % "logback-core",
    "ch.qos.reload4j" % "reload4j",
    "org.slf4j" % "slf4j-log4j12",
    "org.slf4j" % "slf4j-reload4j",
    "io.dropwizard.metrics" % "metrics-logback",
    "log4j" % "log4j"
  )

  // Beam 2.65 pulls in jackson-module-scala_2.12, which pulls in chill_2.12
  def jacksonCrossBuilt(scalaVersion: String): Seq[ExclusionRule] = VersionNumber(
    scalaVersion
  ) match {
    case v if v.matchesSemVer(SemanticSelector("2.12.x")) =>
      Nil
    case _ =>
      Seq(
        ExclusionRule("com.twitter", "chill_2.12"),
        ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala_2.12")
      )
  }
}
