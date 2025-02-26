/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import sbt.*
import sbt.util.CacheImplicits.*
import Keys.*
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtassembly.AssemblyPlugin.autoImport.*
import com.github.sbt.git.SbtGit.GitKeys.gitRemoteRepo
import com.here.bom.Bom
import com.typesafe.tools.mima.core.*
import de.heikoseeberger.sbtheader.CommentCreator
import org.typelevel.scalacoptions.JavaMajorVersion.javaMajorVersion

// To test release candidates, find the beam repo and add it as a resolver
// ThisBuild / resolvers += "apache-beam-staging" at "https://repository.apache.org/content/repositories/"
val beamVendorVersion = "0.1"
val beamVersion = "2.63.0"

// check version used by beam
// https://github.com/apache/beam/blob/v2.63.0/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
val autoServiceVersion = "1.0.1"
val autoValueVersion = "1.9"
val avroVersion = sys.props.getOrElse("avro.version", "1.11.4")
val bigdataossVersion = "2.2.26"
val bigtableClientVersion = "1.28.0"
val commonsCodecVersion = "1.17.1"
val commonsCompressVersion = "1.26.2"
val commonsIoVersion = "2.16.1"
val commonsLang3Version = "3.14.0"
val commonsMath3Version = "3.6.1"
val gcpLibrariesVersion = "26.49.0"
val googleClientsVersion = "2.0.0"
val guavaVersion = "33.1.0-jre"
val hamcrestVersion = "2.1"
val httpClientVersion = "4.5.13"
val httpCoreVersion = "4.4.14"
val jacksonVersion = "2.15.4"
val jodaTimeVersion = "2.10.14"
val nettyVersion = "4.1.110.Final"
val protobufVersion = "3.25.5"
val slf4jVersion = "1.7.30"
val zstdJniVersion = "1.5.6-3"
// dependent versions
val googleApiServicesBigQueryVersion = s"v2-rev20241222-$googleClientsVersion"
val googleApiServicesDataflowVersion = s"v1b3-rev20250106-$googleClientsVersion"
val googleApiServicesPubsubVersion = s"v1-rev20220904-$googleClientsVersion"
val googleApiServicesStorageVersion = s"v1-rev20241206-$googleClientsVersion"
// beam tested versions
val zetasketchVersion = "0.1.0" // sdks/java/extensions/zetasketch/build.gradle
val flinkVersion = "1.19.0" // runners/flink/1.19/build.gradle
val flinkMinorVersion = VersionNumber(flinkVersion).numbers.take(2).mkString(".")
val hadoopVersion = "3.4.1" // sdks/java/io/parquet/build.gradle
val sparkVersion = "3.5.0" // runners/spark/3/build.gradle
val sparkMajorVersion = VersionNumber(sparkVersion).numbers.take(1).mkString(".")

// check recommended versions from libraries-bom
// https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/26.53.0/index.html
val failureAccessVersion = "1.0.2"
val checkerQualVersion = "3.48.3"
val jsr305Version = "3.0.2"
val perfmarkVersion = "0.27.0"

val algebirdVersion = "0.13.10"
val annoy4sVersion = "0.10.0"
val annoyVersion = "0.2.6"
val breezeVersion = "2.1.0"
val caffeineVersion = "2.9.3"
val cassandraDriverVersion = "3.11.5"
val cassandraVersion = "3.11.17"
val catsVersion = "2.13.0"
val chillVersion = "0.10.0"
val circeVersion = "0.14.10"
val commonsTextVersion = "1.10.0"
val elasticsearch7Version = "7.17.21"
val elasticsearch8Version = "8.17.1"
val fansiVersion = "0.5.0"
val featranVersion = "0.8.0"
val httpAsyncClientVersion = "4.1.5"
val jakartaJsonVersion = "2.1.3"
val javaLshVersion = "0.12"
val jedisVersion = "5.2.0"
val jnaVersion = "5.16.0"
val junitInterfaceVersion = "0.13.3"
val junitVersion = "4.13.2"
val kantanCodecsVersion = "0.5.3"
val kantanCsvVersion = "0.7.0"
val kryoVersion = "4.0.3"
val magnoliaVersion = "1.1.10"
val magnolifyVersion = "0.7.4"
val metricsVersion = "4.2.30"
val munitVersion = "1.1.0"
val neo4jDriverVersion = "4.4.19"
val ndArrayVersion = "0.3.3"
val parquetExtraVersion = "0.4.3"
val parquetVersion = "1.15.0"
val pprintVersion = "0.9.0"
val protobufGenericVersion = "0.2.9"
val scalacheckVersion = "1.18.1"
val scalaCollectionCompatVersion = "2.13.0"
val scalaMacrosVersion = "2.1.1"
val scalatestVersion = "3.2.19"
val shapelessVersion = "2.3.12"
val sparkeyVersion = "3.2.5"
val tensorFlowVersion = "0.4.2"
val tensorFlowMetadataVersion = "1.16.1"
val testContainersVersion = "0.41.8"
val voyagerVersion = "2.1.0"

// dependent versions
val algebraVersion = catsVersion // algebra is a cats module
val scalatestplusVersion = s"$scalatestVersion.0"
val scalacheckMinorVersion = VersionNumber(scalacheckVersion).numbers.take(2).mkString("-")

// BOMs
lazy val beamBom = Bom("org.apache.beam" % "beam-sdks-java-bom" % beamVersion)
lazy val gcpBom = Bom("com.google.cloud" % "libraries-bom" % gcpLibrariesVersion)
lazy val guavaBom = Bom("com.google.guava" % "guava-bom" % guavaVersion)
lazy val jacksonBom = Bom("com.fasterxml.jackson" % "jackson-bom" % jacksonVersion)
lazy val magnolifyBom = Bom("com.spotify" % "magnolify-bom" % magnolifyVersion)
lazy val nettyBom = Bom("io.netty" % "netty-bom" % nettyVersion)

val NothingFilter: explicitdeps.ModuleFilter = { _ => false }

// project
ThisBuild / tlBaseVersion := "0.14"
ThisBuild / organization := "com.spotify"
ThisBuild / organizationName := "Spotify AB"
ThisBuild / startYear := Some(2016)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / sonatypeCredentialHost := Sonatype.sonatypeLegacy
ThisBuild / developers := List(
  Developer(
    id = "sinisa_lyh",
    name = "Neville Li",
    email = "neville.lyh@gmail.com",
    url = url("https://twitter.com/sinisa_lyh")
  ),
  Developer(
    id = "ravwojdyla",
    name = "Rafal Wojdyla",
    email = "ravwojdyla@gmail.com",
    url = url("https://twitter.com/ravwojdyla")
  ),
  Developer(
    id = "andrewsmartin",
    name = "Andrew Martin",
    email = "andrewsmartin.mg@gmail.com",
    url = url("https://twitter.com/andrew_martin92")
  ),
  Developer(
    id = "fallonfofallon",
    name = "Fallon Chen",
    email = "fallon@spotify.com",
    url = url("https://twitter.com/fallonfofallon")
  ),
  Developer(
    id = "regadas",
    name = "Filipe Regadas",
    email = "filiperegadas@gmail.com",
    url = url("https://twitter.com/regadas")
  ),
  Developer(
    id = "jto",
    name = "Julien Tournay",
    email = "julient@spotify.com",
    url = url("https://twitter.com/skaalf")
  ),
  Developer(
    id = "clairemcginty",
    name = "Claire McGinty",
    email = "clairem@spotify.com",
    url = url("http://github.com/clairemcginty")
  ),
  Developer(
    id = "syodage",
    name = "Shameera Rathnayaka",
    email = "shameerayodage@gmail.com",
    url = url("http://github.com/syodage")
  ),
  Developer(
    id = "kellen",
    name = "Kellen Dye",
    email = "dye.kellen@gmail.com",
    url = url("http://github.com/kellen")
  ),
  Developer(
    id = "farzad-sedghi",
    name = "farzad sedghi",
    email = "farzadsedghi2@gmail.com",
    url = url("http://github.com/farzad-sedghi")
  )
)

// scala versions
val scala213 = "2.13.16"
val scala212 = "2.12.20"
val scalaDefault = scala213

// compiler settings
ThisBuild / tlJdkRelease := Some(8)
ThisBuild / tlFatalWarnings := false
ThisBuild / scalaVersion := scalaDefault
ThisBuild / crossScalaVersions := Seq(scalaDefault, scala212)

// CI settings
ThisBuild / tlCiHeaderCheck := true
ThisBuild / tlCiScalafmtCheck := true
ThisBuild / tlCiJavafmtCheck := true

// github actions
val java21 = JavaSpec.corretto("21")
val java17 = JavaSpec.corretto("17")
val java11 = JavaSpec.corretto("11")
val javaDefault = java11
val condPrimaryScala = s"matrix.scala == '${CrossVersion.binaryScalaVersion(scalaDefault)}'"
val condPrimaryJava = s"matrix.java == '${javaDefault.render}'"
val condIsMain = "github.ref == 'refs/heads/main'"
val condIsTag = "startsWith(github.ref, 'refs/tags/v')"
val condSkipPR = "github.event_name != 'pull_request'"
val condSkipForkPR = s"($condSkipPR || !github.event.pull_request.head.repo.fork)"

val githubWorkflowCheckStep = WorkflowStep.Sbt(
  List("githubWorkflowCheck"),
  name = Some("Check that workflows are up to date")
)
val githubWorkflowGcpAuthStep = WorkflowStep.Use(
  UseRef.Public("google-github-actions", "auth", "v2"),
  Map(
    "credentials_json" -> "${{ secrets.GCP_CREDENTIALS }}",
    "export_environment_variables" -> "true",
    "create_credentials_file" -> "true"
  ),
  cond = Some(condSkipForkPR),
  name = Some("gcloud auth")
)
val githubWorkflowSetupStep = WorkflowStep.Run(
  List("scripts/gha_setup.sh"),
  name = Some("Setup GitHub Action")
)

val skipUnauthorizedGcpGithubWorkflow = Def.setting {
  githubIsWorkflowBuild.value && sys.props.get("bigquery.project").isEmpty
}

ThisBuild / githubWorkflowTargetBranches := Seq("main")
ThisBuild / githubWorkflowJavaVersions := Seq(javaDefault, java17, java21) // default MUST be head
ThisBuild / githubWorkflowBuildPreamble ++= Seq(githubWorkflowGcpAuthStep, githubWorkflowSetupStep)
ThisBuild / githubWorkflowBuildPostamble ++= Seq(
  WorkflowStep.Sbt(
    List("undeclaredCompileDependenciesTest", "unusedCompileDependenciesTest"),
    name = Some("Check dependencies")
  )
)
ThisBuild / githubWorkflowPublishPreamble ++= Seq(
  WorkflowStep.Sbt(
    List("scio-repl/assembly"),
    name = Some("Package repl")
  )
)
ThisBuild / githubWorkflowPublishPostamble ++= Seq(
  WorkflowStep.Use(
    UseRef.Public("softprops", "action-gh-release", "v1"),
    Map(
      "files" -> "scio-repl/target/scala-2.13/scio-repl.jar",
      "draft" -> "true"
    ),
    name = Some("Upload Repl")
  )
)
ThisBuild / githubWorkflowAddedJobs ++= Seq(
  WorkflowJob(
    "coverage",
    "Test Coverage",
    WorkflowStep.CheckoutFull ::
      WorkflowStep.SetupJava(List(javaDefault)) :::
      List(
        githubWorkflowCheckStep,
        WorkflowStep.Sbt(
          List("coverage", "test", "coverageAggregate"),
          name = Some("Test coverage")
        ),
        WorkflowStep.Use(
          UseRef.Public("codecov", "codecov-action", "v5"),
          Map("token" -> "${{ secrets.CODECOV_TOKEN }}"),
          name = Some("Upload coverage report")
        )
      ),
    scalas = List(CrossVersion.binaryScalaVersion(scalaDefault)),
    javas = List(javaDefault)
  ),
  WorkflowJob(
    "avro-legacy",
    "Test Legacy Avro",
    WorkflowStep.CheckoutFull ::
      WorkflowStep.SetupJava(List(javaDefault)) :::
      List(
        WorkflowStep.Sbt(
          List("scio-avro/test"),
          env = Map("JAVA_OPTS" -> "-Davro.version=1.8.2"),
          name = Some("Test")
        )
      ),
    cond = Some(Seq(condSkipPR, condIsMain).mkString(" && ")),
    scalas = List(CrossVersion.binaryScalaVersion(scalaDefault)),
    javas = List(javaDefault)
  ),
  WorkflowJob(
    "it-test",
    "Integration Test",
    WorkflowStep.CheckoutFull ::
      WorkflowStep.SetupJava(List(javaDefault)) :::
      List(
        githubWorkflowCheckStep,
        githubWorkflowGcpAuthStep,
        githubWorkflowSetupStep
          .withEnv(
            Map(
              "BQ_READ_TIMEOUT" -> "30000",
              "CLOUDSQL_SQLSERVER_PASSWORD" -> "${{ secrets.CLOUDSQL_SQLSERVER_PASSWORD }}"
            )
          ),
        WorkflowStep.Sbt(
          List("set integration/test/skip := false", "integration/test"),
          name = Some("Test")
        )
      ),
    cond = Some(Seq(condSkipPR, condIsMain).mkString(" && ")),
    scalas = List(CrossVersion.binaryScalaVersion(scalaDefault)),
    javas = List(javaDefault)
  ),
  WorkflowJob(
    "site",
    "Generate Site",
    WorkflowStep.CheckoutFull ::
      WorkflowStep.SetupJava(List(javaDefault)) :::
      List(
        githubWorkflowCheckStep,
        githubWorkflowGcpAuthStep,
        WorkflowStep.Run(
          List("scripts/gha_setup.sh"),
          name = Some("Setup GitHub Action")
        ),
        WorkflowStep.Sbt(
          List("scio-examples/compile", "site/makeSite"),
          env = Map("SOCCO" -> "true"),
          name = Some("Generate site")
        ),
        WorkflowStep.Use(
          UseRef.Public("peaceiris", "actions-gh-pages", "v3.9.3"),
          params = Map(
            "github_token" -> "${{ secrets.GITHUB_TOKEN }}",
            "publish_dir" -> {
              val path = (ThisBuild / baseDirectory).value.toPath.toAbsolutePath
                .relativize((site / makeSite / target).value.toPath)
              // os-independent path rendering ...
              (0 until path.getNameCount).map(path.getName).mkString("/")
            },
            "keep_files" -> "true"
          ),
          name = Some("Publish site"),
          cond = Some(Seq(condSkipPR, condIsTag).mkString(" && "))
        )
      ),
    cond = Some(condSkipForkPR),
    scalas = List(CrossVersion.binaryScalaVersion(scalaDefault)),
    javas = List(javaDefault)
  )
)

// mima
ThisBuild / mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.spotify.scio.testing.TransformOverride.ofSource"
  ),
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "com.spotify.scio.options.ScioOptions.setZstdDictionary"
  ),
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "com.spotify.scio.options.ScioOptions.getZstdDictionary"
  ),
  // removal of private classes
  ProblemFilters.exclude[MissingClassProblem](
    "com.spotify.scio.coders.instances.kryo.GaxApiExceptionSerializer"
  ),
  ProblemFilters.exclude[MissingClassProblem](
    "com.spotify.scio.coders.instances.kryo.StatusRuntimeExceptionSerializer"
  ),
  ProblemFilters.exclude[MissingClassProblem](
    "com.spotify.scio.coders.instances.kryo.BigtableRetriesExhaustedExceptionSerializer"
  ),
  // added new Cache.get method
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "com.spotify.scio.util.Cache.get"
  ),
  // added SortedMapCoder
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.spotify.scio.coders.instances.MutableMapCoder.*"
  ),
  ProblemFilters.exclude[DirectAbstractMethodProblem](
    "org.apache.beam.sdk.coders.Coder.verifyDeterministic"
  ),
  ProblemFilters.exclude[DirectAbstractMethodProblem](
    "org.apache.beam.sdk.coders.Coder.getCoderArguments"
  ),
  // added BQ Json object
  ProblemFilters.exclude[MissingTypesProblem](
    "com.spotify.scio.bigquery.types.package$Json$"
  ),
  // tf-metadata upgrade
  ProblemFilters.exclude[Problem](
    "org.tensorflow.metadata.v0.*"
  ),
  // relax type hierarchy for batch stream
  ProblemFilters.exclude[IncompatibleMethTypeProblem](
    "com.spotify.scio.grpc.GrpcBatchDoFn.asyncLookup"
  ),
  // added TableRow syntax
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.spotify.scio.bigquery.syntax.TableRowOps.*"
  ),
  // narrow return type from Map to TableRow
  ProblemFilters.exclude[IncompatibleResultTypeProblem](
    "com.spotify.scio.bigquery.syntax.TableRowOps.getRecord$extension"
  ),
  ProblemFilters.exclude[IncompatibleResultTypeProblem](
    "com.spotify.scio.bigquery.syntax.TableRowOps.getRecord"
  ),
  // narrow return type from Seq to List
  ProblemFilters.exclude[IncompatibleResultTypeProblem](
    "com.spotify.scio.bigquery.syntax.TableRowOps.getRepeated$extension"
  ),
  ProblemFilters.exclude[IncompatibleResultTypeProblem](
    "com.spotify.scio.bigquery.syntax.TableRowOps.getRepeated"
  ),
  // BQ api v1 update
  ProblemFilters.exclude[IncompatibleResultTypeProblem](
    "com.spotify.scio.bigquery.BigQueryStorageTap.*"
  ),
  ProblemFilters.exclude[IncompatibleMethTypeProblem](
    "com.spotify.scio.bigquery.BigQueryStorageTap.*"
  ),
  ProblemFilters.exclude[IncompatibleMethTypeProblem](
    "com.spotify.scio.bigquery.BigQueryTaps.*"
  ),
  ProblemFilters.exclude[IncompatibleResultTypeProblem](
    "com.spotify.scio.bigquery.StorageUtil.tableReadOptions"
  ),
  ProblemFilters.exclude[MissingClassProblem]("com.spotify.scio.tensorflow.PredictDoFn"),
  ProblemFilters.exclude[MissingClassProblem]("com.spotify.scio.tensorflow.PredictDoFn$"),
  ProblemFilters.exclude[MissingClassProblem]("com.spotify.scio.tensorflow.SavedBundlePredictDoFn"),
  ProblemFilters.exclude[MissingClassProblem](
    "com.spotify.scio.tensorflow.SavedBundlePredictDoFn$"
  ),
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.spotify.scio.tensorflow.package.tensorFlowPredictSCollectionOps"
  ),
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.spotify.scio.tensorflow.package.tensorFlowPredictSCollectionOps"
  ),
  ProblemFilters.exclude[MissingClassProblem](
    "com.spotify.scio.tensorflow.syntax.PredictSCollectionOps"
  ),
  ProblemFilters.exclude[MissingClassProblem](
    "com.spotify.scio.tensorflow.syntax.PredictSCollectionOps$"
  ),
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.spotify.scio.tensorflow.syntax.SCollectionSyntax.tensorFlowPredictSCollectionOps"
  ),
  // dropped custom BigQueryAvroUtilsWrapper
  ProblemFilters.exclude[MissingClassProblem](
    "org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtilsWrapper"
  ),
  // Changes in avro SlowGenericRecordCoder
  ProblemFilters.exclude[Problem](
    "com.spotify.scio.coders.avro.SlowGenericRecordCoder*"
  ),
  // tablerow json fix
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.spotify.scio.bigquery.types.package#Json.apply"
  ),
  ProblemFilters.exclude[IncompatibleResultTypeProblem](
    "com.spotify.scio.bigquery.types.package#Json.parse"
  )
)

// headers
lazy val currentYear = java.time.LocalDate.now().getYear
lazy val keepExistingHeader =
  HeaderCommentStyle.cStyleBlockComment.copy(commentCreator = new CommentCreator() {
    override def apply(text: String, existingText: Option[String]): String =
      existingText
        .getOrElse(
          HeaderCommentStyle.cStyleBlockComment.commentCreator(text)
        )
        .trim()
  })

// sbt does not support skip for all tasks
lazy val testSkipped = Def.task {
  if ((Test / test / skip).value) () else (Test / test).value
}
lazy val undeclaredCompileDependenciesTestSkipped = Def.task {
  if ((Compile / compile / skip).value) () else undeclaredCompileDependenciesTest.value
}
lazy val unusedCompileDependenciesTestSkipped = Def.task {
  if ((Compile / compile / skip).value) () else unusedCompileDependenciesTest.value
}

val bomSettings = Def.settings(
  beamBom,
  gcpBom,
  guavaBom,
  jacksonBom,
  magnolifyBom,
  nettyBom,
  dependencyOverrides ++=
    beamBom.key.value.bomDependencies ++
      gcpBom.key.value.bomDependencies ++
      guavaBom.key.value.bomDependencies ++
      jacksonBom.key.value.bomDependencies ++
      magnolifyBom.key.value.bomDependencies ++
      nettyBom.key.value.bomDependencies
)

val commonSettings = bomSettings ++ Def.settings(
  headerLicense := Some(HeaderLicense.ALv2(currentYear.toString, "Spotify AB")),
  headerMappings := headerMappings.value ++ Map(
    HeaderFileType.scala -> keepExistingHeader,
    HeaderFileType.java -> keepExistingHeader
  ),
  scalacOptions ++= ScalacOptions.defaults(scalaVersion.value),
  scalacOptions := {
    val exclude = ScalacOptions
      .tokensForVersion(
        scalaVersion.value,
        Set(
          // too many false positives
          ScalacOptions.privateWarnDeadCode,
          ScalacOptions.warnDeadCode,
          // too many warnings
          ScalacOptions.warnValueDiscard,
          // not ready for scala 3 yet
          ScalacOptions.source3
        )
      )
      .toSet
    scalacOptions.value.filterNot(exclude.contains)
  },
  javacOptions := {
    val exclude = Set(
      // too many warnings
      "-Xlint:all"
    )
    javacOptions.value.filterNot(exclude.contains)
  },
  javaOptions := JavaOptions.defaults(javaMajorVersion),
  resolvers ++= Resolver.sonatypeOssRepos("public"),
  excludeDependencies += Exclude.beamKafka,
  excludeDependencies ++= Exclude.loggerImplementations,
  dependencyOverrides ++= Seq(
    // override when testing with legacy version
    "org.apache.avro" % "avro" % avroVersion,
    "org.apache.avro" % "avro-compiler" % avroVersion,
    // zstd-jni has strict version-scheme, force version
    "com.github.luben" % "zstd-jni" % zstdJniVersion,
    // downgrade deps to align with beam version
    "com.google.auto.value" % "auto-value" % autoValueVersion,
    "com.google.auto.value" % "auto-value-annotations" % autoValueVersion,
    "joda-time" % "joda-time" % jodaTimeVersion,
    "org.apache.httpcomponents" % "httpclient" % httpClientVersion,
    "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion, // slf4j-bom only available for v2
    // remove and let BOM override version after Beam upgrades to 4.x
    // see: https://github.com/spotify/scio/issues/5617
    "com.google.protobuf" % "protobuf-java" % protobufVersion,
    "com.google.protobuf" % "protobuf-java-util" % protobufVersion
  ),
  // libs to help with cross-build
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapelessVersion,
    "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion
  ),
  unusedCompileDependenciesFilter -= Seq(
    moduleFilter("com.chuusai", "shapeless"),
    moduleFilter("org.scala-lang", "scala-reflect"),
    moduleFilter("org.scala-lang.modules", "scala-collection-compat"),
    moduleFilter("org.typelevel", "scalac-compat-annotation")
  ).reduce(_ | _),
  fork := true,
  run / outputStrategy := Some(OutputStrategy.StdoutOutput),
  run / javaOptions ++= JavaOptions.runDefaults(javaMajorVersion),
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  Test / javaOptions ++= JavaOptions.testDefaults(javaMajorVersion),
  Test / testOptions += Tests.Argument("-oD"),
  coverageExcludedPackages := Seq(
    // skip modules
    "com\\.spotify\\.scio\\.examples\\..*",
    "com\\.spotify\\.scio\\.repl\\..*",
    // generated API (22 params)
    "com\\.spotify\\.scio\\.coders\\.instances\\.TupleCoders",
    "com\\.spotify\\.scio\\.smb\\.util\\.SMBMultiJoin",
    "com\\.spotify\\.scio\\.util\\.MultiJoin"
  ).mkString(";"),
  coverageHighlighting := true
)

// for modules containing java jUnit 4 tests
lazy val jUnitSettings = Def.settings(
  libraryDependencies ++= Seq(
    "com.github.sbt" % "junit-interface" % junitInterfaceVersion % Test
  ),
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-a")
)

lazy val macroSettings = Def.settings(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  libraryDependencies ++= {
    VersionNumber(scalaVersion.value) match {
      case v if v.matchesSemVer(SemanticSelector("2.12.x")) =>
        Seq(
          compilerPlugin(
            ("org.scalamacros" % "paradise" % scalaMacrosVersion).cross(CrossVersion.full)
          )
        )
      case _ => Nil
    }
  },
  scalacOptions ++= ScalacOptions.tokensForVersion(
    scalaVersion.value,
    Set(ScalacOptions.macroCacheImplicitSchemas(true))
  )
)

lazy val directRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime
)
lazy val dataflowRunnerDependencies = Seq(
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime
)

lazy val sparkRunnerDependencies = Seq(
  "org.apache.beam" % s"beam-runners-spark-$sparkMajorVersion" % beamVersion % Runtime,
  "org.apache.spark" %% "spark-core" % sparkVersion % Runtime,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Runtime
)

lazy val flinkRunnerDependencies = Seq(
  "org.apache.beam" % s"beam-runners-flink-$flinkMinorVersion" % beamVersion % Runtime,
  "org.apache.flink" % "flink-clients" % flinkVersion % Runtime,
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % Runtime
)
lazy val beamRunners = settingKey[String]("beam runners")
lazy val beamRunnersEval = settingKey[Seq[ModuleID]]("beam runners")

def beamRunnerSettings: Seq[Setting[_]] = Seq(
  beamRunners := "",
  beamRunnersEval := {
    Option(beamRunners.value)
      .filter(_.nonEmpty)
      .orElse(sys.props.get("beamRunners"))
      .orElse(sys.env.get("BEAM_RUNNERS"))
      .map(_.split(","))
      .map {
        _.flatMap {
          case "DirectRunner"   => directRunnerDependencies
          case "DataflowRunner" => dataflowRunnerDependencies
          case "SparkRunner"    => sparkRunnerDependencies
          case "FlinkRunner"    => flinkRunnerDependencies
          case _                => Nil
        }.toSeq
      }
      .getOrElse(directRunnerDependencies)
  },
  libraryDependencies ++= beamRunnersEval.value
)

val protocJavaSourceManaged =
  settingKey[File]("Default directory for java sources generated by protoc.")
val protocGrpcSourceManaged =
  settingKey[File]("Default directory for gRPC sources generated by protoc.")

def avroSettings(scope: Configuration): Seq[Setting[_]] = Def.settings(
  SbtAvro.autoImport.avroVersion := avroVersion,
  avroAdditionalDependencies ~= { modules =>
    modules.map { m =>
      m.configurations match {
        case None if scope != Compile => m.withConfigurations(Some(scope.name))
        case _                        => m
      }
    }
  }
)

lazy val protobufSettings = Def.settings(
  PB.protocVersion := gcpBom.key.value.version("com.google.protobuf" % "protobuf-java"),
  protocJavaSourceManaged := sourceManaged.value / "compiled_proto",
  protocGrpcSourceManaged := sourceManaged.value / "compiled_grpc",
  libraryDependencies ++= Seq(
    "io.grpc" % "protoc-gen-grpc-java" % gcpBom.key.value asProtocPlugin (),
    "com.google.protobuf" % "protobuf-java" % gcpBom.key.value % "protobuf",
    "com.google.protobuf" % "protobuf-java" % gcpBom.key.value
  )
) ++ Seq(Compile, Test).flatMap(c => inConfig(c)(protobufConfigSettings))

lazy val protobufConfigSettings = Def.settings(
  PB.targets := Seq(
    PB.gens.java(PB.protocVersion.value) -> Defaults.configSrcSub(protocJavaSourceManaged).value,
    PB.gens.plugin("grpc-java") -> Defaults.configSrcSub(protocGrpcSourceManaged).value
  ),
  managedSourceDirectories ++= PB.targets.value.map(_.outputPath)
)

def splitTests(tests: Seq[TestDefinition], filter: Seq[String], forkOptions: ForkOptions) = {
  val (filtered, default) = tests.partition(test => filter.contains(test.name))
  val policy = Tests.SubProcess(forkOptions)
  new Tests.Group(name = "<default>", tests = default, runPolicy = policy) +: filtered.map { test =>
    new Tests.Group(name = test.name, tests = Seq(test), runPolicy = policy)
  }
}

lazy val scio = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .settings(commonSettings)
  .settings(
    assembly / aggregate := false
  )
  .aggregate(
    `integration`,
    `scio-avro`,
    `scio-bom`,
    `scio-cassandra3`,
    `scio-core`,
    `scio-elasticsearch-common`,
    `scio-elasticsearch7`,
    `scio-elasticsearch8`,
    `scio-examples`,
    `scio-extra`,
    `scio-google-cloud-platform`,
    `scio-grpc`,
    `scio-jdbc`,
    `scio-jmh`,
    `scio-macros`,
    `scio-neo4j`,
    `scio-parquet`,
    `scio-redis`,
    `scio-repl`,
    `scio-smb`,
    `scio-snowflake`,
    `scio-tensorflow`,
    `scio-test-core`,
    `scio-test-google-cloud-platform`,
    `scio-test-parquet`,
    `scio-test`
  )

lazy val `scio-bom` = project
  .in(file("scio-bom"))
  .enablePlugins(BillOfMaterialsPlugin)
  .disablePlugins(TypelevelSettingsPlugin)
  .settings(
    // Just one BOM including all cross Scala versions
    crossVersion := CrossVersion.disabled,
    // Create BOM in the first run
    crossScalaVersions := Seq(scalaDefault),
    bomIncludeProjects := Seq(
      `scio-avro`,
      `scio-cassandra3`,
      `scio-core`,
      `scio-elasticsearch-common`,
      `scio-elasticsearch7`,
      `scio-elasticsearch8`,
      `scio-extra`,
      `scio-google-cloud-platform`,
      `scio-grpc`,
      `scio-jdbc`,
      `scio-macros`,
      `scio-neo4j`,
      `scio-parquet`,
      `scio-redis`,
      `scio-repl`,
      `scio-smb`,
      `scio-tensorflow`,
      `scio-test-core`,
      `scio-test-google-cloud-platform`,
      `scio-test-parquet`,
      `scio-test`
    ),
    // pom project. No ABI
    tlMimaPreviousVersions := Set.empty
  )

lazy val `scio-core` = project
  .in(file("scio-core"))
  .enablePlugins(BuildInfoPlugin, SbtAvro)
  .dependsOn(`scio-macros`)
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(avroSettings(Test))
  .settings(protobufSettings)
  .settings(
    description := "Scio - A Scala API for Apache Beam and Google Cloud Dataflow",
    Compile / resources ++= Seq(
      (ThisBuild / baseDirectory).value / "build.sbt",
      (ThisBuild / baseDirectory).value / "version.sbt"
    ),
    // required by service-loader
    unusedCompileDependenciesFilter -= moduleFilter("com.google.auto.service", "auto-service"),
    libraryDependencies ++= Seq(
      // compile
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.github.luben" % "zstd-jni" % zstdJniVersion,
      "com.google.api" % "gax" % gcpBom.key.value,
      "com.google.api-client" % "google-api-client" % gcpBom.key.value,
      "com.google.auto.service" % "auto-service-annotations" % autoServiceVersion,
      "com.google.auto.service" % "auto-service" % autoServiceVersion,
      "com.google.code.findbugs" % "jsr305" % jsr305Version,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % gcpBom.key.value,
      "com.google.http-client" % "google-http-client-gson" % gcpBom.key.value,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" % "chill-protobuf" % chillVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" %% "chill-algebird" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "io.grpc" % "grpc-api" % gcpBom.key.value,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-protobuf" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.apache.commons" % "commons-compress" % commonsCompressVersion,
      "org.apache.commons" % "commons-lang3" % commonsLang3Version,
      "org.apache.commons" % "commons-math3" % commonsMath3Version,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.typelevel" %% "algebra" % algebraVersion,
      // provided
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % Provided,
      "com.google.apis" % "google-api-services-dataflow" % googleApiServicesDataflowVersion % Provided,
      "org.apache.beam" % s"beam-runners-flink-$flinkMinorVersion" % beamVersion % Provided,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Provided,
      "org.apache.beam" % s"beam-runners-spark-$sparkMajorVersion" % beamVersion % Provided,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion % Provided,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided,
      "com.google.cloud.bigdataoss" % "gcs-connector" % s"hadoop2-$bigdataossVersion" % Provided,
      "com.google.cloud.bigdataoss" % "gcsio" % bigdataossVersion % Provided,
      "com.google.cloud.bigdataoss" % "util-hadoop" % s"hadoop2-$bigdataossVersion" % Provided,
      // test
      "com.lihaoyi" %% "fansi" % fansiVersion % Test,
      "com.lihaoyi" %% "pprint" % pprintVersion % Test,
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion % Test,
      "com.spotify" % "annoy" % annoyVersion % Test,
      "com.spotify" %% "magnolify-guava" % magnolifyVersion % Test,
      "com.twitter" %% "chill" % chillVersion % Test,
      "commons-io" % "commons-io" % commonsIoVersion % Test,
      "junit" % "junit" % junitVersion % Test,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Test,
      "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13" % Test,
      "org.hamcrest" % "hamcrest" % hamcrestVersion % Test,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test,
      "org.scalactic" %% "scalactic" % scalatestVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.scalatestplus" %% s"scalacheck-$scalacheckMinorVersion" % scalatestplusVersion % Test,
      "org.typelevel" %% "cats-kernel" % catsVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    ),
    buildInfoKeys := Seq[BuildInfoKey](scalaVersion, version, "beamVersion" -> beamVersion),
    buildInfoPackage := "com.spotify.scio"
  )

lazy val `scio-test` = project
  .in(file("scio-test"))
  .dependsOn(
    `scio-test-core`,
    `scio-test-google-cloud-platform`,
    `scio-test-parquet`
  )
  .settings(commonSettings)
  .settings(
    description := "Scio helpers for ScalaTest",
    // only releases after 0.14.4
    tlMimaPreviousVersions := tlMimaPreviousVersions.value
      .filter(v => VersionNumber(v).numbers.last >= 4)
  )

lazy val `scio-test-core` = project
  .in(file("scio-test/core"))
  .dependsOn(`scio-core`)
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio helpers for ScalaTest",
    undeclaredCompileDependenciesFilter -= moduleFilter("org.scalatest"),
    unusedCompileDependenciesFilter -= moduleFilter("org.scalatest", "scalatest"),
    libraryDependencies ++= Seq(
      "com.google.http-client" % "google-http-client" % gcpBom.key.value, // TODO should we have this here ?
      "com.google.http-client" % "google-http-client-gson" % gcpBom.key.value, // TODO should we have this here ?
      "com.lihaoyi" %% "fansi" % fansiVersion,
      "com.lihaoyi" %% "pprint" % pprintVersion,
      "com.twitter" %% "chill" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.avro" % "avro" % avroVersion, // TODO should we have this here ?
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.hamcrest" % "hamcrest" % hamcrestVersion,
      "org.scalactic" %% "scalactic" % scalatestVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion,
      "org.typelevel" %% "cats-kernel" % catsVersion,
      // runtime
      "junit" % "junit" % junitVersion % Runtime,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
      // test
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    ),
    // only releases after 0.14.4
    tlMimaPreviousVersions := tlMimaPreviousVersions.value
      .filter(v => VersionNumber(v).numbers.last >= 4)
  )

lazy val `scio-test-google-cloud-platform` = project
  .in(file("scio-test/google-cloud-platform"))
  .dependsOn(
    `scio-core`,
    `scio-test-core` % "compile;runtime->runtime"
  )
  .settings(commonSettings)
  .settings(
    description := "Scio helpers for ScalaTest",
    undeclaredCompileDependenciesFilter -= moduleFilter("org.scalatest"),
    unusedCompileDependenciesFilter -= moduleFilter("org.scalatest", "scalatest"),
    libraryDependencies ++= Seq(
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % gcpBom.key.value,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "org.scalatest" %% "scalatest" % scalatestVersion,
      "org.typelevel" %% "cats-kernel" % catsVersion,
      // test
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    ),
    // only releases after 0.14.4
    tlMimaPreviousVersions := tlMimaPreviousVersions.value
      .filter(v => VersionNumber(v).numbers.last >= 4)
  )

lazy val `scio-test-parquet` = project
  .in(file("scio-test/parquet"))
  .dependsOn(
    `scio-core`,
    `scio-parquet`,
    `scio-test-core` % "compile;runtime->runtime",
    `scio-avro` % "test->test",
    `scio-tensorflow` % Provided
  )
  .settings(commonSettings)
  .settings(
    description := "Scio helpers for ScalaTest",
    // only releases after 0.14.4
    tlMimaPreviousVersions := tlMimaPreviousVersions.value
      .filter(v => VersionNumber(v).numbers.last >= 4),
    libraryDependencies ++= Seq(
      "com.spotify" %% "magnolify-parquet" % magnolifyVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.parquet" % "parquet-common" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.tensorflow" % "tensorflow-core-api" % tensorFlowVersion % Provided
    )
  )

lazy val `scio-macros` = project
  .in(file("scio-macros"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio macros",
    libraryDependencies ++= Seq(
      // compile
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion
    )
  )

lazy val `scio-avro` = project
  .in(file("scio-avro"))
  .enablePlugins(SbtAvro)
  .dependsOn(
    `scio-core` % "compile->compile;test->test"
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(avroSettings(Compile))
  .settings(
    description := "Scio add-on for working with Avro",
    libraryDependencies ++= Seq(
      // compile
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "me.lyh" %% "protobuf-generic" % protobufGenericVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-avro" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // test
      "com.spotify" %% "magnolify-cats" % magnolifyVersion % Test,
      "com.spotify" %% "magnolify-scalacheck" % magnolifyVersion % Test,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Test,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.scalatestplus" %% s"scalacheck-$scalacheckMinorVersion" % scalatestplusVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test,
      "org.typelevel" %% "cats-core" % catsVersion % Test
    ),
    Test / unmanagedSourceDirectories += {
      val base = (Test / sourceDirectory).value
      if (avroVersion.startsWith("1.8")) base / "scala-avro-legacy" else base / "scala-avro-latest"
    }
  )

lazy val `scio-google-cloud-platform` = project
  .in(file("scio-google-cloud-platform"))
  .dependsOn(
    `scio-core` % "compile;test->test",
    `scio-avro`
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(jUnitSettings)
  .settings(beamRunnerSettings)
  .settings(
    description := "Scio add-on for Google Cloud Platform",
    unusedCompileDependenciesFilter -= Seq(
      // required for patching jackson version pulled by arrow
      moduleFilter("org.apache.arrow", "arrow-vector"),
      moduleFilter("com.fasterxml.jackson.datatype", "jackson-datatype-jsr310")
    ).reduce(_ | _),
    Test / javaOptions += "-Doverride.type.provider=com.spotify.scio.bigquery.validation.SampleOverrideTypeProvider",
    libraryDependencies ++= Seq(
      // compile
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "com.google.api" % "gax" % gcpBom.key.value,
      "com.google.api" % "gax-grpc" % gcpBom.key.value,
      "com.google.api-client" % "google-api-client" % gcpBom.key.value,
      "com.google.api.grpc" % "grpc-google-cloud-pubsub-v1" % gcpBom.key.value,
      "com.google.api.grpc" % "proto-google-cloud-bigquerystorage-v1" % gcpBom.key.value,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-admin-v2" % gcpBom.key.value,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % gcpBom.key.value,
      "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % gcpBom.key.value,
      "com.google.api.grpc" % "proto-google-cloud-pubsub-v1" % gcpBom.key.value,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.google.auth" % "google-auth-library-credentials" % gcpBom.key.value,
      "com.google.auth" % "google-auth-library-oauth2-http" % gcpBom.key.value,
      "com.google.cloud" % "google-cloud-bigquerystorage" % gcpBom.key.value,
      "com.google.cloud" % "google-cloud-bigtable" % gcpBom.key.value,
      "com.google.cloud" % "google-cloud-core" % gcpBom.key.value,
      "com.google.cloud" % "google-cloud-spanner" % gcpBom.key.value,
      "com.google.cloud.bigdataoss" % "util" % bigdataossVersion,
      "com.google.cloud.bigtable" % "bigtable-client-core" % bigtableClientVersion,
      "com.google.cloud.bigtable" % "bigtable-client-core-config" % bigtableClientVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % gcpBom.key.value,
      "com.google.http-client" % "google-http-client-gson" % gcpBom.key.value,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-java" % chillVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "io.grpc" % "grpc-api" % gcpBom.key.value,
      "io.grpc" % "grpc-auth" % gcpBom.key.value,
      "io.grpc" % "grpc-netty" % gcpBom.key.value,
      "io.grpc" % "grpc-stub" % gcpBom.key.value,
      "io.netty" % "netty-handler" % nettyVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // test
      "com.google.cloud" % "google-cloud-storage" % gcpBom.key.value % Test,
      "com.spotify" %% "magnolify-cats" % magnolifyVersion % Test,
      "com.spotify" %% "magnolify-scalacheck" % magnolifyVersion % Test,
      "org.hamcrest" % "hamcrest" % hamcrestVersion % Test,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.scalatestplus" %% s"scalacheck-$scalacheckMinorVersion" % scalatestplusVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test,
      "org.typelevel" %% "cats-core" % catsVersion % Test,
      "org.scalameta" %% "munit" % munitVersion % Test
    )
  )

lazy val `scio-cassandra3` = project
  .in(file("scio-cassandra/cassandra3"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for Apache Cassandra 3.x",
    libraryDependencies ++= Seq(
      // compile
      "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverVersion,
      "com.esotericsoftware" % "kryo-shaded" % kryoVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.twitter" % "chill-java" % chillVersion,
      "com.twitter" %% "chill" % chillVersion,
      "org.apache.cassandra" % "cassandra-all" % cassandraVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      // test
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    )
  )

lazy val `scio-elasticsearch-common` = project
  .in(file("scio-elasticsearch/common"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    libraryDependencies ++= Seq(
      // compile
      "commons-io" % "commons-io" % commonsIoVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "jakarta.json" % "jakarta.json-api" % jakartaJsonVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.apache.httpcomponents" % "httpasyncclient" % httpAsyncClientVersion,
      "org.apache.httpcomponents" % "httpclient" % httpClientVersion,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // provided
      "co.elastic.clients" % "elasticsearch-java" % elasticsearch8Version % Provided,
      "org.elasticsearch.client" % "elasticsearch-rest-client" % elasticsearch8Version % Provided,
      // test
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    )
  )

lazy val `scio-elasticsearch7` = project
  .in(file("scio-elasticsearch/es7"))
  .dependsOn(
    `scio-elasticsearch-common`
  )
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    unusedCompileDependenciesFilter -= moduleFilter("co.elastic.clients", "elasticsearch-java"),
    libraryDependencies ++= Seq(
      "co.elastic.clients" % "elasticsearch-java" % elasticsearch7Version
    )
  )

lazy val `scio-elasticsearch8` = project
  .in(file("scio-elasticsearch/es8"))
  .dependsOn(
    `scio-elasticsearch-common`
  )
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for writing to Elasticsearch",
    unusedCompileDependenciesFilter -= moduleFilter("co.elastic.clients", "elasticsearch-java"),
    libraryDependencies ++= Seq(
      "co.elastic.clients" % "elasticsearch-java" % elasticsearch8Version
    )
  )

lazy val `scio-extra` = project
  .in(file("scio-extra"))
  .enablePlugins(SbtAvro)
  .dependsOn(
    `scio-core` % "compile;provided->provided;test->test",
    `scio-avro`,
    `scio-google-cloud-platform`,
    `scio-macros`
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(avroSettings(Test))
  .settings(
    description := "Scio extra utilities",
    libraryDependencies ++= Seq(
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.google.zetasketch" % "zetasketch" % zetasketchVersion,
      "com.nrinaudo" %% "kantan.codecs" % kantanCodecsVersion,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "com.spotify" % "annoy" % annoyVersion,
      "com.spotify" % "voyager" % voyagerVersion,
      "com.spotify.sparkey" % "sparkey" % sparkeyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "net.java.dev.jna" % "jna" % jnaVersion, // used by annoy4s
      "net.pishen" %% "annoy4s" % annoy4sVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sketching" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-zetasketch" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.scalanlp" %% "breeze" % breezeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.typelevel" %% "algebra" % algebraVersion,
      // test
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % Test,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    ),
    Compile / doc / sources := List(), // suppress warnings
    compileOrder := CompileOrder.JavaThenScala
  )

lazy val `scio-grpc` = project
  .in(file("scio-grpc"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(protobufSettings)
  .settings(
    description := "Scio add-on for gRPC",
    unusedCompileDependenciesFilter -= moduleFilter("com.google.protobuf", "protobuf-java"),
    libraryDependencies ++= Seq(
      // compile
      "com.google.guava" % "failureaccess" % failureAccessVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.twitter" %% "chill" % chillVersion,
      "io.grpc" % "grpc-api" % gcpBom.key.value,
      "io.grpc" % "grpc-stub" % gcpBom.key.value,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.commons" % "commons-lang3" % commonsLang3Version,
      // test
      "io.grpc" % "grpc-netty" % gcpBom.key.value % Test
    )
  )

lazy val `scio-jdbc` = project
  .in(file("scio-jdbc"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for JDBC",
    libraryDependencies ++= Seq(
      // compile
      "com.google.auto.service" % "auto-service-annotations" % autoServiceVersion,
      "commons-codec" % "commons-codec" % commonsCodecVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion
    )
  )

lazy val `scio-neo4j` = project
  .in(file("scio-neo4j"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for Neo4J",
    libraryDependencies ++= Seq(
      // compile
      "com.spotify" %% "magnolify-neo4j" % magnolifyVersion,
      "com.spotify" %% "magnolify-shared" % magnolifyVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-neo4j" % beamVersion,
      "org.neo4j.driver" % "neo4j-java-driver" % neo4jDriverVersion
    )
  )

val ensureSourceManaged = taskKey[Unit]("ensureSourceManaged")

lazy val `scio-parquet` = project
  .in(file("scio-parquet"))
  .dependsOn(
    `scio-core` % "compile;test->test",
    `scio-tensorflow` % "provided",
    `scio-avro` % "test->test"
  )
  .settings(commonSettings)
  .settings(
    // change annotation processor output directory so IntelliJ can pick them up
    ensureSourceManaged := IO.createDirectory(sourceManaged.value / "main"),
    Compile / compile := Def.task {
      val _ = ensureSourceManaged.value
      (Compile / compile).value
    }.value,
    javacOptions ++= Seq("-s", (sourceManaged.value / "main").toString),
    description := "Scio add-on for Parquet",
    unusedCompileDependenciesFilter -= Seq(
      // required by me.lyh:parquet-avro
      moduleFilter("org.apache.avro", "avro-compiler"),
      // replacing log4j compile time dependency
      moduleFilter("org.slf4j", "log4j-over-slf4j")
    ).reduce(_ | _),
    libraryDependencies ++= Seq(
      // compile
      "com.google.auth" % "google-auth-library-oauth2-http" % gcpBom.key.value,
      "com.google.cloud.bigdataoss" % "util-hadoop" % s"hadoop2-$bigdataossVersion",
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.spotify" %% "magnolify-parquet" % magnolifyVersion,
      "com.twitter" %% "chill" % chillVersion,
      "me.lyh" %% "parquet-avro" % parquetExtraVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.avro" % "avro-compiler" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-hadoop-common" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-hadoop-format" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.parquet" % "parquet-common" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion, // log4j is excluded from hadoop
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // provided
      "org.tensorflow" % "tensorflow-core-api" % tensorFlowVersion % Provided,
      // runtime
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Runtime excludeAll (Exclude.metricsCore),
      "io.dropwizard.metrics" % "metrics-core" % metricsVersion % Runtime,
      // test
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    )
  )

lazy val `scio-snowflake` = project
  .in(file("scio-snowflake"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(
    description := "Scio add-on for Snowflake",
    libraryDependencies ++= Seq(
      // compile
      "com.nrinaudo" %% "kantan.codecs" % kantanCodecsVersion,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-snowflake" % beamVersion
    ),
    tlMimaPreviousVersions := Set.empty // TODO: remove once released
  )

val tensorFlowMetadataSourcesDir =
  settingKey[File]("Directory containing TensorFlow metadata proto files")
val tensorFlowMetadata = taskKey[Seq[File]]("Retrieve TensorFlow metadata proto files")

lazy val `scio-tensorflow` = project
  .in(file("scio-tensorflow"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(protobufSettings)
  .settings(
    description := "Scio add-on for TensorFlow",
    unusedCompileDependenciesFilter -= Seq(
      // used by generated code, excluded above
      moduleFilter("com.google.protobuf", "protobuf-java")
    ).reduce(_ | _),
    libraryDependencies ++= Seq(
      // compile
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.apache.commons" % "commons-compress" % commonsCompressVersion,
      "org.tensorflow" % "tensorflow-core-api" % tensorFlowVersion,
      // test
      "com.spotify" %% "featran-core" % featranVersion % Test,
      "com.spotify" %% "featran-scio" % featranVersion % Test,
      "com.spotify" %% "featran-tensorflow" % featranVersion % Test,
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    ),
    Compile / tensorFlowMetadataSourcesDir := target.value / s"metadata-$tensorFlowMetadataVersion",
    Compile / PB.protoSources += (Compile / tensorFlowMetadataSourcesDir).value,
    Compile / tensorFlowMetadata := {
      def work(tensorFlowMetadataVersion: String) = {
        val tfMetadata = url(
          s"https://github.com/tensorflow/metadata/archive/refs/tags/v$tensorFlowMetadataVersion.zip"
        )
        IO.unzipURL(tfMetadata, target.value, "*.proto").toSeq
      }

      val cacheStoreFactory = streams.value.cacheStoreFactory
      val root = (Compile / tensorFlowMetadataSourcesDir).value
      val tracker =
        Tracked.inputChanged(cacheStoreFactory.make("input")) { (versionChanged, version: String) =>
          val cached = Tracked.outputChanged(cacheStoreFactory.make("output")) {
            (outputChanged: Boolean, files: Seq[HashFileInfo]) =>
              if (versionChanged || outputChanged) work(version)
              else files.map(_.file)
          }
          cached(() => (root ** "*.proto").get().map(FileInfo.hash(_)))
        }

      tracker(tensorFlowMetadataVersion)
    },
    Compile / PB.unpackDependencies := {
      val protoFiles = (Compile / tensorFlowMetadata).value
      val root = (Compile / tensorFlowMetadataSourcesDir).value
      val metadataDep = ProtocPlugin.UnpackedDependency(protoFiles, Seq.empty)
      val deps = (Compile / PB.unpackDependencies).value
      new ProtocPlugin.UnpackedDependencies(deps.mappedFiles ++ Map(root -> metadataDep))
    }
  )

lazy val `scio-examples` = project
  .in(file("scio-examples"))
  .enablePlugins(NoPublishPlugin)
  .disablePlugins(ScalafixPlugin)
  .dependsOn(
    `scio-core` % "compile->test",
    `scio-avro` % "compile->test",
    `scio-google-cloud-platform`,
    `scio-jdbc`,
    `scio-extra`,
    `scio-elasticsearch8`,
    `scio-neo4j`,
    `scio-tensorflow`,
    `scio-smb`,
    `scio-redis`,
    `scio-parquet`
  )
  .settings(commonSettings)
  .settings(soccoSettings)
  .settings(jUnitSettings)
  .settings(beamRunnerSettings)
  .settings(macroSettings)
  .settings(
    compile / skip := skipUnauthorizedGcpGithubWorkflow.value,
    test / skip := skipUnauthorizedGcpGithubWorkflow.value,
    Test / test := testSkipped.value,
    undeclaredCompileDependenciesTest := undeclaredCompileDependenciesTestSkipped.value,
    unusedCompileDependenciesTest := unusedCompileDependenciesTestSkipped.value,
    scalacOptions := {
      val exclude = ScalacOptions
        .tokensForVersion(
          scalaVersion.value,
          Set(ScalacOptions.warnUnused, ScalacOptions.privateWarnUnused)
        )
        .toSet
      scalacOptions.value.filterNot(exclude.contains)
    },
    undeclaredCompileDependenciesFilter -= Seq(
      // missing from gcpBom. Add explicitly once there
      moduleFilter("com.google.cloud.datastore", "datastore-v1-proto-client")
    ).reduce(_ | _),
    unusedCompileDependenciesFilter -= Seq(
      // used in es example
      moduleFilter("com.fasterxml.jackson.datatype", "jackson-datatype-jsr310"),
      // used in beam java
      moduleFilter("com.google.oauth-client", "google-oauth-client"),
      // class reference only
      moduleFilter("mysql", "mysql-connector-java")
    ).reduce(_ | _),
    libraryDependencies ++= Seq(
      // compile
      "co.elastic.clients" % "elasticsearch-java" % elasticsearch8Version,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.google.api-client" % "google-api-client" % gcpBom.key.value,
      "com.google.api.grpc" % "proto-google-cloud-bigtable-v2" % gcpBom.key.value,
      "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % gcpBom.key.value,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.google.apis" % "google-api-services-pubsub" % googleApiServicesPubsubVersion,
      "com.google.auth" % "google-auth-library-credentials" % gcpBom.key.value,
      "com.google.auth" % "google-auth-library-oauth2-http" % gcpBom.key.value,
      "com.google.cloud.bigdataoss" % "util" % bigdataossVersion,
      "com.google.code.findbugs" % "jsr305" % jsr305Version,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % gcpBom.key.value,
      "com.google.oauth-client" % "google-oauth-client" % gcpBom.key.value,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.mysql" % "mysql-connector-j" % "9.2.0",
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "com.spotify" %% "magnolify-avro" % magnolifyVersion,
      "com.spotify" %% "magnolify-bigtable" % magnolifyVersion,
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion,
      "com.spotify" %% "magnolify-guava" % magnolifyVersion,
      "com.spotify" %% "magnolify-neo4j" % magnolifyVersion,
      "com.spotify" %% "magnolify-parquet" % magnolifyVersion,
      "com.spotify" %% "magnolify-shared" % magnolifyVersion,
      "com.spotify" %% "magnolify-tensorflow" % magnolifyVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "me.lyh" %% "parquet-avro" % parquetExtraVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-avro" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.httpcomponents" % "httpcore" % httpCoreVersion,
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.parquet" % "parquet-common" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.neo4j.driver" % "neo4j-java-driver" % neo4jDriverVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.tensorflow" % "tensorflow-core-api" % tensorFlowVersion,
      "redis.clients" % "jedis" % jedisVersion,
      // runtime
      "com.google.cloud.bigdataoss" % "gcs-connector" % s"hadoop2-$bigdataossVersion" % Runtime,
      "com.google.cloud.sql" % "mysql-socket-factory-connector-j-8" % "1.23.0" % Runtime,
      // test
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test
    ),
    // exclude problematic sources if we don't have GCP credentials
    unmanagedSources / excludeFilter := {
      if (BuildCredentials.exists) {
        HiddenFileFilter
      } else {
        HiddenFileFilter ||
        "TypedBigQueryTornadoes*.scala" ||
        "TypedStorageBigQueryTornadoes*.scala" ||
        "RunPreReleaseIT.scala"
      }
    },
    Compile / doc / sources := List(),
    Test / testGrouping := splitTests(
      (Test / definedTests).value,
      List("com.spotify.scio.examples.WordCountTest"),
      ForkOptions().withRunJVMOptions((Test / javaOptions).value.toVector)
    )
  )

lazy val `scio-repl` = project
  .in(file("scio-repl"))
  .dependsOn(
    `scio-core`,
    `scio-google-cloud-platform`,
    `scio-extra`
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    // drop repl compatibility with java 8
    tlJdkRelease := Some(11),
    unusedCompileDependenciesFilter -= Seq(
      moduleFilter("org.scala-lang", "scala-compiler"),
      moduleFilter("org.scalamacros", "paradise")
    ).reduce(_ | _),
    libraryDependencies ++= Seq(
      // compile
      "com.nrinaudo" %% "kantan.codecs" % kantanCodecsVersion,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion,
      "commons-io" % "commons-io" % commonsIoVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion excludeAll (Exclude.gcsio),
      "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // runtime
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Runtime
    ),
    libraryDependencies ++= {
      VersionNumber(scalaVersion.value) match {
        case v if v.matchesSemVer(SemanticSelector("2.12.x")) =>
          Seq("org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full)
        case _ =>
          Nil
      }
    },
    assembly / assemblyJarName := "scio-repl.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy ~= { old =>
      {
        case PathList("org", "apache", "beam", "sdk", _*) =>
          // prefer original beam sdk classes instead of the ones packaged by the runners-direct-java
          CustomMergeStrategy("BeamSdk") { conflicts =>
            import sbtassembly.Assembly._
            val sdkDeps = conflicts.filterNot {
              case Library(module, _, _, _) =>
                module.organization == "org.apache.beam" && module.name == "beam-runners-direct-java"
              case _ => false
            }

            sdkDeps.toList match {
              case Library(_, _, t, s) :: Nil => Right(Vector(JarEntry(t, s)))
              case Project(_, _, t, s) :: Nil => Right(Vector(JarEntry(t, s)))
              case _ =>
                val conflictList = conflicts.mkString("\n  ", "\n  ", "\n")
                Left("Error merging beam sdk classes:" + conflictList)
            }
          }
        case PathList("com", "squareup", _*) =>
          // prefer jvm jar in case of conflict
          CustomMergeStrategy("SquareUp") { conflicts =>
            import sbtassembly.Assembly._
            if (conflicts.size == 1) {
              Right(conflicts.map(conflict => JarEntry(conflict.target, conflict.stream)))
            } else {
              conflicts.collectFirst {
                case Library(ModuleCoordinate(_, jar, _), _, t, s) if jar.endsWith("-jvm") =>
                  JarEntry(t, s)
              } match {
                case Some(e) => Right(Vector(e))
                case None =>
                  val conflictList = conflicts.mkString("\n  ", "\n  ", "\n")
                  Left("Error merging squareup classes:" + conflictList)
              }
            }
          }
        case PathList("dev", "ludovic", "netlib", "InstanceBuilder.class") =>
          // arbitrary pick last conflicting InstanceBuilder
          MergeStrategy.last
        case s if s.endsWith(".proto") =>
          // arbitrary pick last conflicting proto file
          MergeStrategy.last
        case PathList("git.properties" | "arrow-git.properties") =>
          // drop conflicting git properties
          MergeStrategy.discard
        case PathList(segments @ _*) if segments.last == "module-info.class" =>
          // drop conflicting module-info.class
          MergeStrategy.discard
        case PathList("META-INF", "gradle", "incremental.annotation.processors") =>
          // drop conflicting kotlin compiler info
          MergeStrategy.discard
        case PathList("META-INF", "kotlin-project-structure-metadata.json") =>
          // drop conflicting kotlin compiler info
          MergeStrategy.discard
        case PathList("META-INF", tail @ _*) if tail.last.endsWith(".kotlin_module") =>
          // drop conflicting kotlin compiler info
          MergeStrategy.discard
        case PathList("commonMain", _*) =>
          // drop conflicting squareup linkdata
          MergeStrategy.discard
        case PathList("mozilla", "public-suffix-list.txt") =>
          // drop conflicting suffix lists from beam-vendor-grpc, httpclient
          MergeStrategy.discard
        case PathList("META-INF", "io.netty.versions.properties") =>
          // merge conflicting netty property files
          MergeStrategy.filterDistinctLines
        case PathList("META-INF", "native-image", "native-image.properties") =>
          // merge conflicting native-image property files
          MergeStrategy.filterDistinctLines
        case PathList(
              "META-INF",
              "native-image",
              "io.grpc.netty.shaded.io.netty",
              "netty-codec" | "netty-handler",
              "generated",
              "handlers",
              "reflect-config.json"
            ) =>
          // merge conflicting netty config files
          MergeStrategy.filterDistinctLines
        case s => old(s)
      }
    }
  )

lazy val `scio-jmh` = project
  .in(file("scio-jmh"))
  .enablePlugins(JmhPlugin)
  .enablePlugins(NoPublishPlugin)
  .dependsOn(
    `scio-core`,
    `scio-avro`
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio JMH Microbenchmarks",
    Jmh / sourceDirectory := (Test / sourceDirectory).value,
    Jmh / classDirectory := (Test / classDirectory).value,
    Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
    unusedCompileDependenciesFilter := NothingFilter,
    libraryDependencies ++= directRunnerDependencies ++ Seq(
      // test
      "org.hamcrest" % "hamcrest" % hamcrestVersion % Test,
      "org.slf4j" % "slf4j-nop" % slf4jVersion % Test
    )
  )

lazy val `scio-smb` = project
  .in(file("scio-smb"))
  .enablePlugins(SbtAvro)
  .dependsOn(
    `scio-core` % "compile;test->test",
    `scio-avro` % "provided;test->test",
    `scio-google-cloud-platform` % "provided",
    `scio-parquet` % "provided",
    `scio-tensorflow` % "provided"
  )
  .settings(commonSettings)
  .settings(jUnitSettings)
  .settings(beamRunnerSettings)
  .settings(avroSettings(Provided))
  .settings(
    description := "Sort Merge Bucket source/sink implementations for Apache Beam",
    unusedCompileDependenciesFilter -= Seq(
      // replacing log4j compile time dependency
      moduleFilter("org.slf4j", "log4j-over-slf4j")
    ).reduce(_ | _),
    libraryDependencies ++= Seq(
      // compile
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.google.auto.service" % "auto-service-annotations" % autoServiceVersion,
      "com.google.auto.value" % "auto-value-annotations" % autoValueVersion,
      "com.google.code.findbugs" % "jsr305" % jsr305Version,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.spotify" %% "magnolify-parquet" % magnolifyVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      // #3260 work around for sorter memory limit until we patch upstream
      // "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
      "org.apache.beam" % "beam-vendor-guava-32_1_2-jre" % beamVendorVersion,
      "org.apache.commons" % "commons-lang3" % commonsLang3Version,
      "org.checkerframework" % "checker-qual" % checkerQualVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion, // log4j is excluded from hadoop
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // provided
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion % Provided, // scio-gcp
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion % Provided,
      "org.apache.beam" % "beam-sdks-java-extensions-avro" % beamVersion % Provided, // scio-avro
      "org.apache.beam" % "beam-sdks-java-extensions-protobuf" % beamVersion % Provided, // scio-tensorflow
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion % Provided, // scio-gcp
      "org.apache.beam" % "beam-sdks-java-io-hadoop-common" % beamVersion % Provided, // scio-parquet
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided, // scio-parquet
      "org.apache.parquet" % "parquet-avro" % parquetVersion % Provided, // scio-parquet
      "org.apache.parquet" % "parquet-column" % parquetVersion % Provided, // scio-parquet
      "org.apache.parquet" % "parquet-common" % parquetVersion % Provided, // scio-parquet
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion % Provided, // scio-parquet
      "org.tensorflow" % "tensorflow-core-api" % tensorFlowVersion % Provided, // scio-tensorflow
      // test
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion % Test classifier "tests",
      "org.hamcrest" % "hamcrest" % hamcrestVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    ),
    javacOptions ++= {
      (Compile / sourceManaged).value.mkdirs()
      Seq("-s", (Compile / sourceManaged).value.getAbsolutePath)
    },
    compileOrder := CompileOrder.JavaThenScala
  )

lazy val `scio-redis` = project
  .in(file("scio-redis"))
  .dependsOn(
    `scio-core` % "compile;test->test"
  )
  .settings(commonSettings)
  .settings(
    description := "Scio integration with Redis",
    libraryDependencies ++= Seq(
      // compile
      "com.softwaremill.magnolia1_2" %% "magnolia" % magnoliaVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-redis" % beamVersion,
      "redis.clients" % "jedis" % jedisVersion,
      // test
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    )
  )

lazy val integration = project
  .in(file("integration"))
  .enablePlugins(NoPublishPlugin, SbtAvro)
  .dependsOn(
    `scio-core` % "compile;test->test",
    `scio-avro` % "test->test",
    `scio-cassandra3` % "test->test",
    `scio-elasticsearch8` % "test->test",
    `scio-extra` % "test->test",
    `scio-google-cloud-platform` % "compile;test->test",
    `scio-jdbc` % "compile;test->test",
    `scio-neo4j` % "test->test",
    `scio-smb` % "test->provided,test"
  )
  .settings(commonSettings)
  .settings(jUnitSettings)
  .settings(macroSettings)
  .settings(avroSettings(Compile))
  .settings(
    // disable compile / test when unauthorized
    compile / skip := skipUnauthorizedGcpGithubWorkflow.value,
    test / skip := true,
    Test / test := {
      val logger = streams.value.log
      if ((Test / test / skip).value) {
        logger.warn(
          "integration/test are skipped.\n" +
            "Run 'set integration/test/skip := false' to run them"
        )
      }
      testSkipped.value
    },
    undeclaredCompileDependenciesTest := undeclaredCompileDependenciesTestSkipped.value,
    unusedCompileDependenciesTest := unusedCompileDependenciesTestSkipped.value,
    libraryDependencies ++= Seq(
      // compile
      "com.google.api-client" % "google-api-client" % gcpBom.key.value,
      "com.google.apis" % "google-api-services-bigquery" % googleApiServicesBigQueryVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.http-client" % "google-http-client" % gcpBom.key.value,
      "com.google.protobuf" % "protobuf-java" % gcpBom.key.value,
      "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11",
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      // runtime
      "com.google.cloud.sql" % "cloud-sql-connector-jdbc-sqlserver" % "1.23.0" % Runtime,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Runtime,
      // test
      "com.dimafeng" %% "testcontainers-scala-elasticsearch" % testContainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-neo4j" % testContainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion % Test,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion % Test,
      "com.spotify" %% "magnolify-datastore" % magnolifyVersion % Test,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Test
    )
  )

// =======================================================================
// Site settings
// =======================================================================
lazy val site = project
  .in(file("site"))
  .enablePlugins(
    ParadoxSitePlugin,
    ParadoxMaterialThemePlugin,
    GhpagesPlugin,
    ScalaUnidocPlugin,
    SiteScaladocPlugin,
    MdocPlugin
  )
  .dependsOn(
    `scio-avro` % "compile->test",
    `scio-cassandra3`,
    `scio-core` % "compile->test",
    `scio-elasticsearch-common`,
    `scio-elasticsearch8`,
    `scio-extra`,
    `scio-google-cloud-platform`,
    `scio-grpc` % "compile->test",
    `scio-jdbc`,
    `scio-macros`,
    `scio-neo4j`,
    `scio-parquet`,
    `scio-redis`,
    `scio-smb`,
    `scio-tensorflow`,
    `scio-test-core`
  )
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    description := "Scio - Documentation",
    fork := false,
    publish / skip := true,
    autoAPIMappings := true,
    gitRemoteRepo := "git@github.com:spotify/scio.git",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "com.nrinaudo" %% "kantan.csv" % kantanCsvVersion
    ),
    // unidoc
    ScalaUnidoc / siteSubdirName := "api",
    ScalaUnidoc / scalacOptions := Seq.empty,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
      `scio-avro`,
      `scio-cassandra3`,
      `scio-core`,
      `scio-elasticsearch-common`,
      `scio-elasticsearch8`,
      `scio-extra`,
      `scio-google-cloud-platform`,
      `scio-grpc`,
      `scio-jdbc`,
      `scio-neo4j`,
      `scio-parquet`,
      `scio-redis`,
      `scio-smb`,
      `scio-tensorflow`,
      `scio-test-core`,
      `scio-test-google-cloud-platform`,
      `scio-test-parquet`,
      `scio-test`
    ),
    // unidoc handles class paths differently than compile and may give older
    // versions high precedence.
    ScalaUnidoc / unidoc / unidocAllClasspaths := (ScalaUnidoc / unidoc / unidocAllClasspaths).value
      .map { cp =>
        cp.filterNot(_.data.getCanonicalPath.matches(""".*guava-11\..*"""))
          .filterNot(_.data.getCanonicalPath.matches(""".*bigtable-client-core-0\..*"""))
      },
    // mdoc
    // pre-compile md using mdoc
    Compile / scalacOptions ~= { _.filterNot(_.startsWith("-Wconf")) },
    mdocIn := (paradox / sourceDirectory).value,
    mdocExtraArguments ++= Seq("--no-link-hygiene"),
    // paradox
    Compile / paradox / sourceManaged := mdocOut.value,
    paradoxProperties ++= Map(
      "extref.example.base_url" -> "https://spotify.github.io/scio/examples/%s.scala.html",
      "github.base_url" -> "https://github.com/spotify/scio",
      "javadoc.com.google.api.services.bigquery.base_url" -> "https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/",
      "javadoc.com.google.common.hash.base_url" -> s"https://guava.dev/releases/$guavaVersion/api/docs",
      "javadoc.com.spotify.scio.base_url" -> "http://spotify.github.com/scio/api",
      "javadoc.org.apache.avro.base_url" -> "https://avro.apache.org/docs/current/api/java/",
      "javadoc.org.apache.beam.base_url" -> s"https://beam.apache.org/releases/javadoc/$beamVersion",
      "javadoc.org.apache.beam.sdk.extensions.smb.base_url" -> "https://spotify.github.io/scio/api/org/apache/beam/sdk/extensions/smb",
      "javadoc.org.joda.time.base_url" -> "https://www.joda.org/joda-time/apidocs",
      "javadoc.org.tensorflow.base_url" -> "https://www.tensorflow.org/jvm/api_docs/java/",
      "javadoc.org.tensorflow.link_style" -> "direct",
      "scaladoc.com.spotify.scio.base_url" -> "https://spotify.github.io/scio/api",
      "scaladoc.com.twitter.algebird.base_url" -> "https://twitter.github.io/algebird/api/",
      "scaladoc.kantan.base_url" -> "https://nrinaudo.github.io/kantan.csv/api"
    ),
    Compile / paradoxMaterialTheme := ParadoxMaterialTheme()
      .withFavicon("images/favicon.ico")
      .withColor("white", "indigo")
      .withLogo("images/logo.png")
      .withCopyright(s"Copyright (C) $currentYear Spotify AB")
      .withRepository(uri("https://github.com/spotify/scio"))
      .withSocial(uri("https://github.com/spotify"), uri("https://twitter.com/spotifyeng")),
    // sbt-site
    addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, ScalaUnidoc / siteSubdirName),
    makeSite / mappings ++= Seq(
      file("scio-examples/target/site/index.html") -> "examples/index.html"
    ) ++ SoccoIndex.mappings,
    makeSite := makeSite.dependsOn(mdoc.toTask("")).value
  )

lazy val soccoIndex = taskKey[File]("Generates examples/index.html")
lazy val soccoSettings = if (sys.env.contains("SOCCO")) {
  Seq(
    scalacOptions ++= Seq(
      "-P:socco:out:scio-examples/target/site",
      "-P:socco:package_com.spotify.scio:https://spotify.github.io/scio/api"
    ),
    autoCompilerPlugins := true,
    addCompilerPlugin(("io.regadas" %% "socco-ng" % "0.1.14").cross(CrossVersion.full)),
    // Generate scio-examples/target/site/index.html
    soccoIndex := SoccoIndex.generate(target.value / "site" / "index.html"),
    Compile / compile := {
      val _ = soccoIndex.value
      (Compile / compile).value
    }
  )
} else {
  Nil
}
