import sbt.*
import Keys.*

import scala.sys.process.*
import scala.util.matching.Regex.Match

object MakeBom {
  val BeamVendored = "beam-vendor-"
  val DefaultBeamVendoredVersion = "0.1"
  val BeamOrganization = "org.apache.beam"
  def toCamel(name: String): String =
    "([-.])([0-9a-z])".r.replaceAllIn(name.toLowerCase, (m: Match) => m.group(2).toUpperCase)

  val NameOverrides: Map[(String, String), (String, String, String) => String] = Map(
    ("me.lyh", "parquet-avro") -> {(org, name, rev) => "meLyhParquetAvro" },
    ("co.elastic.clients", "elasticsearch-java") -> { (_, _, rev) =>s"elasticsearchJava${rev.head}" }
  )

  trait Flub {
    def org: String
    def name: String
    def rev: String

    def camel: String
    def versionVariable: String = s"${camel}Version"
    def versionString: String = s"""lazy val ${versionVariable} = "${rev}""""
    def oper: String
    def depString: String = {
      val variable = s"lazy val ${camel.capitalize}"
      s"""$variable = "${org}" $oper "${name}" % "$versionVariable""""
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
    def isExcluded: Boolean = m.configurations.exists(_.startsWith("plugin->")) // plugin deps
    def camel: String =
      NameOverrides.get(m.key) match {
        case Some(fn) => fn(m.organization, m.name, m.revision)
        case None => toCamel(m.name)
      }
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
    val multipleVersions = inModules.filter(_._2.size > 1)
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
            case org :: name :: rev :: _ => Some((org, name) -> rev)
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

    val versions = (beamModules ++ nonTransitive).toList.map(_.versionString) ++
      transitiveBuildDeps.map(_.versionString)
    val deps = (beamModules ++ nonTransitive).toList.map(_.depString) ++
      transitiveBuildDeps.map(_.depString)

    print(versions.distinct.sorted.mkString("\n"))
    print(deps.distinct.sorted.mkString("\n"))

    // return original state
    state
  }
}
