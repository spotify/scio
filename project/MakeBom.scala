import sbt.*
import Keys.*

import java.io.FileWriter
import scala.sys.process.*
import scala.util.matching.Regex.Match

object MakeBom {
  case class Output(path: String, packageName: Option[String] = None)
  val Outputs = List(
    Output("project/Libraries.scala"),
    Output("scio-bom/src/main/scala/com/spotify/scio/Libraries.scala", Some("com.spotify.scio"))
  )
  val Template = "import sbt._\n\nobject Libraries {\n%s\n}\n"
  val DefaultBeamVendoredVersion = "0.1"
  val BeamOrganization = "org.apache.beam"
  val BeamVendored = "beam-vendor-"
  val BeamVendoredVersionVariable = "beamVendoredVersion"
  val BeamVersionVariable = "beamVersion"
  val ScalaLangOrganization = "org.scala-lang"

  private def toCamel(name: String): String =
    "([-.])([0-9a-z])".r.replaceAllIn(name.toLowerCase, (m: Match) => m.group(2).toUpperCase)

  private def versionStr(v: String, r: String) = s"""lazy val ${v} = "${r}""""

  val NameOverrides: Map[(String, String), (String, String, String) => String] = Map(
    ("me.lyh", "parquet-avro") -> { (_, _, _) => "meLyhParquetAvro" },
    ("com.google.cloud.bigdataoss", "util") -> { (_, _, _) => "bigdataosssUtil" },
    ("co.elastic.clients", "elasticsearch-java") -> { (_, _, rev) => s"elasticsearchJava${rev(0)}" }
  )

  trait Flub {
    def org: String
    def name: String
    def rev: String

    def camel: String
    def versionVariable: String = {
      s"${camel}Version"
    }
    def versionString: String = versionStr(versionVariable, rev)
    def oper: String
    def depRegex(s: String) =
      s""""$org" $oper "$name" %([^%,\n]*)""".r.replaceAllIn(s, m => {
        val suffix = if(m.group(1).endsWith(" ")) " " else ""
        s"Libraries.${camel.capitalize}$suffix"
      })
    def depString: String = {
      val variable = s"lazy val ${camel.capitalize}"
      s"""$variable = "$org" $oper "$name" % $versionVariable"""
    }
  }

  implicit class TupleModule(m: ((String, String), String)) extends Flub {
    override val ((org, name), rev) = m
    val camel: String = toCamel(name)
    val oper: String = "%"
  }

  implicit class ModuleModule(m: ModuleID) extends Flub {
    override val (org, name, rev) = (m.organization, m.name, m.revision)
    def key: (String, String) = (m.organization, m.name)
    def isBeam: Boolean = m.organization.startsWith(BeamOrganization)
    def isBeamVendored: Boolean = isBeam && m.name.startsWith(BeamVendored)
    def isScalaDep: Boolean = m.crossVersion.isInstanceOf[Binary]
    def isExcluded: Boolean = {
      m.organization == ScalaLangOrganization ||
      m.configurations.isDefined // ignores provided, test, and plugin dependencies
      // m.configurations.exists { c => c == "provided" || c == "test" || c.startsWith("plugin->") }
    }
    def camel: String =
      NameOverrides.get(m.key) match {
        case Some(fn) => fn(m.organization, m.name, m.revision)
        case None => toCamel(m.name)
      }
    override def versionVariable: String =
      if(isBeamVendored) BeamVendoredVersionVariable
      else if(isBeam) BeamVersionVariable
      else super.versionVariable
    def oper: String = if (isScalaDep) "%%" else "%"
  }

  def makeBom = Command.args("makeBom", "beamVersion [beamVendorVersion]") { (state, args) =>
    val (beamVersion, beamVendorVersion) = args match {
      case beamVersion :: beamVendorVersion :: Nil => (beamVersion, beamVendorVersion)
      case beamVersion :: Nil => (beamVersion, DefaultBeamVendoredVersion)
      case _ => throw new IllegalArgumentException("Missing beamVersion")
    }

    val structure = Project.structure(state)

    // get all modules depended-upon by all directly-aggregated subprojects
    val inModules = state.currentProject.aggregate
      .filter { ref => ref != Project.current(state) } // ignore root
      .flatMap { ref => (ref / libraryDependencies).get(structure.data).getOrElse(Seq.empty) }
      .filterNot(_.isExcluded)
      .groupBy(_.key)
      .mapValues(_.toSet)

    // if we have conflicts in the build
    val multipleVersions = inModules
      .mapValues(_.map(_.revision))
      .filter(_._2.size > 1)
    if(multipleVersions.nonEmpty)
      println(s"Projects do not agree on: ${multipleVersions}")

    val modules = inModules.flatMap(_._2)

    // versions to assert for beam dependencies
    val beamModules = modules.collect {
      case m if m.isBeam =>
        val rev = if(m.isBeamVendored) beamVendorVersion else beamVersion
        m.withRevision(rev)
    }.toSet

    // for all beam modules, pull their transitive dependencies
    val transitiveDeps = beamModules
      .flatMap { m =>
        val output = s"coursier resolve ${m.organization}:${m.name}:${m.revision}" lineStream_!

        output.flatMap { dep =>
          dep.split(":").toList match {
            case org :: name :: rev :: _ if org != BeamOrganization => Some((org, name) -> rev)
            case _ => None
          }
        }
        .toList
      }
      .groupBy(_._1)
      // take max version supported by some part of beam
      .mapValues(_.map(_._2).max)

    // get the set of overlapping modules for our declared and beam-transitive deps
    val transitiveKeys = transitiveDeps.keySet.intersect(modules.map(_.key).toSet)
    // retain the list of non-transitive deps from the build
    val nonTransitive = modules.filterNot { m => transitiveKeys.contains(m.key) }
    // retain the transitive deps on which we explicitly depends
    val transitiveBuildDeps = transitiveDeps.filter { case (k, v) => transitiveKeys.contains(k) }

    val versions = List(
      versionStr(BeamVersionVariable, beamVersion),
      versionStr(BeamVendoredVersionVariable, beamVendorVersion)
    ) ++
      nonTransitive.toList.map(_.versionString) ++
      transitiveBuildDeps.map(_.versionString)

    val deps = (beamModules ++ nonTransitive).toList.map(_.depString) ++
      transitiveBuildDeps.map(_.depString)

    val contents = Template.format(
      versions.distinct.sorted.mkString("  ", "\n  ", "\n") +
        deps.distinct.sorted.mkString("\n  ", "\n  ", "")
    )
    Outputs.foreach { case Output(path, optPackage) =>
      val w = new FileWriter(new File(path))
      optPackage.foreach(pkg => w.write(s"package $pkg\n\n") )
      w.write(contents)
      w.close()
    }

    val buildSbtSource = scala.io.Source.fromFile("build.sbt")
    val buildSbtContents = buildSbtSource.mkString
    buildSbtSource.close()

    val x = (beamModules ++ nonTransitive).toList
      .foldLeft(buildSbtContents) { case (str, m) => m.depRegex(str) }
    val y = transitiveBuildDeps.foldLeft(x) { case (str, m) => m.depRegex(str) }
    val w = new FileWriter(new File("build.sbt"))
    w.write(y)
    w.close()

    // return original state
    state
  }
}
