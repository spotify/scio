import sbt.*
import sbt.Keys.*
import sbt.internal.ProjectMatrix
import scalafix.sbt.ScalafixTestkitPlugin.autoImport.*

case class ConfigAxis(idSuffix: String, directorySuffix: String) extends VirtualAxis.WeakAxis {}

case class ScalafixProject(input: Project, output: Project, scioAxis: VirtualAxis) {
  def scalafixRow(pm: ProjectMatrix, rules: Project): ProjectMatrix = {
    pm.customRow(
      scalaVersions = Seq(_root_.scalafix.sbt.BuildInfo.scala212),
      axisValues = Seq(scioAxis, VirtualAxis.jvm),
      _.settings(
        moduleName := name.value + scioAxis.idSuffix,
        scalafixTestkitOutputSourceDirectories := (output / Compile / unmanagedSourceDirectories).value,
        scalafixTestkitInputSourceDirectories := (input / Compile / unmanagedSourceDirectories).value,
        scalafixTestkitInputClasspath := (input / Compile / fullClasspath).value
      ).dependsOn(rules)
    )
  }
}

object ScalafixProject {
  implicit class ScalafixRowImplicit(pm: ProjectMatrix) {
    def scalafixRows(scalafixes: List[ScalafixProject], rules: Project): ProjectMatrix = {
      scalafixes match {
        case Nil => pm
        case head :: tail => head.scalafixRow(pm, rules).scalafixRows(tail, rules)
      }
    }
  }
}
